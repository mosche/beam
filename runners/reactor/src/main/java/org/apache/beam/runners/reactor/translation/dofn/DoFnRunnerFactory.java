/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.reactor.translation.dofn;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.beam.runners.core.construction.ParDoTranslation.getSchemaInformation;
import static org.apache.beam.sdk.util.SerializableUtils.deserializeFromByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;

import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import reactor.core.publisher.Mono;

public abstract class DoFnRunnerFactory<InT, T> {
  abstract static class RunnerWithTeardown<InT, T> implements DoFnRunner<InT, T> {

    public void teardown() {
      DoFnInvokers.invokerFor(getFn()).invokeTeardown();
    }
  }

  /**
   * Creates a runner that is ready to process elements.
   *
   * <p>Both, {@link org.apache.beam.sdk.transforms.reflect.DoFnInvoker#invokeSetup setup} and
   * {@link DoFnRunner#startBundle()} are already invoked by the factory.
   */
  abstract Mono<RunnerWithTeardown<InT, T>> create(
      LocalPipelineOptions opts, MetricsContainer metrics, OutputManager output);

  /**
   * Fuses the factory for the following {@link DoFnRunner} into a single factory that processes
   * both DoFns in a single step.
   */
  abstract <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next);

  // FIXME remove input? We shouldn't need the input coder.
  public static <InT, T> DoFnRunnerFactory<InT, T> simple(
      AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
      PCollection<InT> input,
      Mono<SideInputReader> sideInputReader) {
    return new SimpleFactory<>(appliedPT, input, sideInputReader);
  }

  private static class SimpleFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    final ParDo.MultiOutput<InT, T> transform;
    final DoFnSchemaInformation schemaInformation;
    final @Nullable byte[] serializedDoFn;
    final PCollection<InT> input;
    final Mono<SideInputReader> sideInputs;

    SimpleFactory(
        AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
        PCollection<InT> input,
        Mono<SideInputReader> sideInputs) {
      this.transform = appliedPT.getTransform();
      this.serializedDoFn =
          requiresCopy(transform.getFn()) ? serializeToByteArray(transform.getFn()) : null;
      this.schemaInformation = getSchemaInformation(appliedPT);
      this.input = input;
      this.sideInputs = sideInputs;
    }

    private static boolean requiresCopy(DoFn<?, ?> fn) {
      DoFnSignature sig = DoFnSignatures.signatureForDoFn(fn);
      return sig.startBundle() != null
          || sig.finishBundle() != null
          || sig.teardown() != null
          || sig.setup() != null
          || sig.usesState();
    }

    private DoFn<InT, T> doFn() {
      return serializedDoFn != null
          ? (DoFn<InT, T>) deserializeFromByteArray(serializedDoFn, "DoFn")
          : transform.getFn();
    }

    @Override
    Mono<RunnerWithTeardown<InT, T>> create(
        LocalPipelineOptions opts, MetricsContainer metrics, OutputManager output) {
      return sideInputs.map(resolved -> create(resolved, opts, metrics, output));
    }

    private RunnerWithTeardown<InT, T> create(
        SideInputReader resolvedSideInputs,
        LocalPipelineOptions opts,
        MetricsContainer metrics,
        OutputManager output) {
      List<TupleTag<?>> additionalOuts = transform.getAdditionalOutputTags().getAll();
      TupleTag<T> mainOut = transform.getMainOutputTag();
      DoFnRunner<InT, T> simpleRunner =
          DoFnRunners.simpleRunner(
              opts,
              doFn(),
              resolvedSideInputs,
              additionalOuts.isEmpty() ? output : new FilteredOutput(output, mainOut),
              mainOut,
              additionalOuts,
              NoOpStepContext.INSTANCE,
              input.getCoder(),
              EMPTY_MAP, // no coders used
              WindowingStrategy.globalDefault(),
              schemaInformation,
              ImmutableMap.of());

      RunnerWithTeardown<InT, T> runner =
          opts.isMetricsEnabled()
              ? new DoFnRunnerWithMetrics<>(simpleRunner, metrics)
              : new Delegate<>(simpleRunner);

      // Invoke setup and then startBundle before returning the runner
      DoFnInvokers.tryInvokeSetupFor(runner.getFn(), opts);
      try {
        runner.startBundle();
      } catch (RuntimeException re) {
        runner.teardown();
        throw re;
      }
      return runner;
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      return new FusedFactory<>(Lists.newArrayList(this, next));
    }

    /**
     * Delegate {@link OutputManager} that only forwards outputs matching the provided tag. This is
     * used in cases where unused, obsolete outputs get dropped to avoid unnecessary caching.
     */
    private static class FilteredOutput implements OutputManager {
      final OutputManager outputManager;
      final TupleTag<?> tupleTag;

      FilteredOutput(OutputManager outputManager, TupleTag<?> tupleTag) {
        this.outputManager = outputManager;
        this.tupleTag = tupleTag;
      }

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        if (tupleTag.equals(tag)) {
          outputManager.output(tag, output);
        }
      }
    }
  }

  /**
   * Factory that produces a fused runner consisting of multiple chained {@link DoFn DoFns}. Outputs
   * are directly forwarded to the next runner without buffering inbetween.
   */
  private static class FusedFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private final List<DoFnRunnerFactory<?, ?>> factories;

    FusedFactory(List<DoFnRunnerFactory<?, ?>> factories) {
      this.factories = factories;
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      factories.add(next);
      return (DoFnRunnerFactory<InT, T2>) this;
    }

    @Override
    @SuppressWarnings("rawtypes")
    Mono<RunnerWithTeardown<InT, T>> create(
        LocalPipelineOptions opts, MetricsContainer metrics, OutputManager out) {
      int size = factories.size();
      // FIXME does this need a scheduler?
      Mono<RunnerWithTeardown[]> runners = Mono.just(new RunnerWithTeardown[size]);
      for (int pos = size - 1; pos >= 0; pos--) {
        runners = runners.flatMap(new InitRunner(pos, opts, metrics, out));
      }
      return runners.map(FusedRunner::new);
    }

    /** Initializer for runner at {@link #pos} */
    @SuppressWarnings("rawtypes")
    private class InitRunner implements Function<RunnerWithTeardown[], Mono<RunnerWithTeardown[]>> {
      final int pos;
      final LocalPipelineOptions opts;
      final MetricsContainer metrics;
      final OutputManager out;

      InitRunner(int pos, LocalPipelineOptions opts, MetricsContainer metrics, OutputManager out) {
        this.pos = pos;
        this.opts = opts;
        this.metrics = metrics;
        this.out = out;
      }

      @Override
      public Mono<RunnerWithTeardown[]> apply(RunnerWithTeardown[] runners) {
        // Last runner uses output, every other one fuses it's output directly into the next runner
        OutputManager out = pos + 1 < runners.length ? new FusedOut(runners[pos + 1]) : this.out;
        return factories
            .get(pos)
            .create(opts, metrics, out)
            .map(
                runner -> {
                  runners[pos] = runner;
                  return runners;
                });
      }
    }

    /** {@link OutputManager} that forwards output directly to the next runner. */
    private static class FusedOut implements OutputManager {
      final RunnerWithTeardown<?, ?> runner;

      FusedOut(RunnerWithTeardown<?, ?> runner) {
        this.runner = runner;
      }

      @Override
      public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
        runner.processElement((WindowedValue) output);
      }
    }

    private static class FusedRunner<InT, T> extends RunnerWithTeardown<InT, T> {
      final RunnerWithTeardown<?, ?>[] runners;

      FusedRunner(RunnerWithTeardown<?, ?>[] runners) {
        this.runners = runners;
      }

      @Override
      public void startBundle() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].startBundle();
        }
      }

      @Override
      public void processElement(WindowedValue<InT> elem) {
        runners[0].processElement((WindowedValue) elem);
      }

      @Override
      public <KeyT> void onTimer(
          String timerId,
          String timerFamilyId,
          KeyT key,
          BoundedWindow window,
          Instant timestamp,
          Instant outputTimestamp,
          TimeDomain timeDomain) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void finishBundle() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].finishBundle();
        }
      }

      @Override
      public DoFn<InT, T> getFn() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void teardown() {
        for (int i = 0; i < runners.length; i++) {
          runners[i].teardown();
        }
      }
    }
  }

  private static class Delegate<InT, T> extends RunnerWithTeardown<InT, T> {
    private final DoFnRunner<InT, T> runner;

    Delegate(DoFnRunner<InT, T> runner) {
      this.runner = runner;
    }

    @Override
    public void startBundle() {
      runner.startBundle();
    }

    @Override
    public void processElement(WindowedValue<InT> elem) {
      runner.processElement(elem);
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      runner.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
    }

    @Override
    public void finishBundle() {
      runner.finishBundle();
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
      runner.onWindowExpiration(window, timestamp, key);
    }

    @Override
    public DoFn<InT, T> getFn() {
      return runner.getFn();
    }
  }

  private static class NoOpStepContext implements StepContext {
    static final StepContext INSTANCE = new NoOpStepContext();

    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException("stateInternals is not supported");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("timerInternals is not supported");
    }
  }
}
