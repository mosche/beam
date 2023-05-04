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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import reactor.core.publisher.Mono;

public abstract class DoFnRunnerFactory<InT, T> {
  interface RunnerWithTeardown<InT, T> extends DoFnRunner<InT, T> {
    void teardown();

    boolean isSDF();
  }

  /**
   * Creates a runner that is ready to process elements.
   *
   * <p>Both, {@link org.apache.beam.sdk.transforms.reflect.DoFnInvoker#invokeSetup setup} and
   * {@link DoFnRunner#startBundle()} are already invoked by the factory.
   */
  abstract Mono<RunnerWithTeardown<InT, T>> create(
      OutputManager output, @Nullable MetricsContainer metrics);

  /**
   * Fuses the factory for the following {@link DoFnRunner} into a single factory that processes
   * both DoFns in a single step.
   */
  abstract <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next);

  /** This factory produces an SDF runner */
  abstract boolean isSDF();

  // FIXME remove input? We shouldn't need the input coder.
  public static <InT, T> DoFnRunnerFactory<InT, T> simple(
      LocalPipelineOptions opts,
      AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
      PCollection<InT> input,
      Mono<SideInputReader> sideInputReader) {
    return new SimpleFactory<>(opts, appliedPT, input, sideInputReader);
  }

  private static class SimpleFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private static final PTransformMatcher SPLITTABLE_MATCHER =
        PTransformMatchers.parDoWithFnType(SplittableParDoNaiveBounded.NaiveProcessFn.class);

    final AtomicInteger nextId = new AtomicInteger();
    final LocalPipelineOptions opts;
    final ParDo.MultiOutput<InT, T> transform;
    final boolean isSDF;
    final DoFnSchemaInformation schemaInformation;
    final byte @Nullable [] serializedDoFn;
    final PCollection<InT> input;
    final Mono<SideInputReader> sideInputs;

    SimpleFactory(
        LocalPipelineOptions opts,
        AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
        PCollection<InT> input,
        Mono<SideInputReader> sideInputs) {
      this.opts = opts;
      this.transform = appliedPT.getTransform();
      this.serializedDoFn =
          requiresCopy(opts, transform.getFn()) ? serializeToByteArray(transform.getFn()) : null;
      this.schemaInformation = getSchemaInformation(appliedPT);
      this.input = input;
      this.sideInputs = opts.getParallelism() > 1 ? sideInputs.cache() : sideInputs;
      this.isSDF = SPLITTABLE_MATCHER.matches(appliedPT); // fuse all but SDFs
    }

    private static boolean requiresCopy(LocalPipelineOptions opts, DoFn<?, ?> fn) {
      if (opts.getParallelism() == 1) {
        return false;
      }
      DoFnSignature sig = DoFnSignatures.signatureForDoFn(fn);
      return sig.startBundle() != null
          || sig.finishBundle() != null
          || sig.teardown() != null
          || sig.setup() != null
          || sig.usesState();
    }

    private DoFn<InT, T> getDoFnInstance() {
      return serializedDoFn != null
          ? (DoFn<InT, T>) deserializeFromByteArray(serializedDoFn, "DoFn")
          : transform.getFn();
    }

    @Override
    boolean isSDF() {
      return isSDF;
    }

    @Override
    Mono<RunnerWithTeardown<InT, T>> create(
        OutputManager output, @Nullable MetricsContainer metrics) {
      return sideInputs.map(resolved -> create(resolved, output, metrics));
    }

    private RunnerWithTeardown<InT, T> create(
        SideInputReader resolvedSideInputs,
        OutputManager output,
        @Nullable MetricsContainer metrics) {
      List<TupleTag<?>> additionalOuts = transform.getAdditionalOutputTags().getAll();
      TupleTag<T> mainOut = transform.getMainOutputTag();
      DoFnRunner<InT, T> runner =
          DoFnRunners.simpleRunner(
              opts,
              getDoFnInstance(),
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
      // Invoke setup and then startBundle before returning the runner
      DoFnInvokers.tryInvokeSetupFor(runner.getFn(), opts);
      return new MetricsRunner<>(runner, this, metrics);
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      return new FusedFactory<>(Lists.newArrayList(this, next), isSDF || next.isSDF());
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
    private boolean isSDF;

    FusedFactory(List<DoFnRunnerFactory<?, ?>> factories, boolean isSDF) {
      this.factories = factories;
      this.isSDF = isSDF;
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      factories.add(next);
      return (DoFnRunnerFactory<InT, T2>) this;
    }

    @Override
    boolean isSDF() {
      return isSDF;
    }

    @Override
    @SuppressWarnings("rawtypes")
    Mono<RunnerWithTeardown<InT, T>> create(OutputManager out, @Nullable MetricsContainer metrics) {
      int size = factories.size();
      Mono<RunnerWithTeardown[]> runners = Mono.just(new RunnerWithTeardown[size]);
      for (int pos = size - 1; pos >= 0; pos--) {
        runners = runners.flatMap(new InitRunner(pos, out, metrics));
      }
      return runners.map(FusedRunner::new);
    }

    /** Initializer for runner at {@link #pos} */
    @SuppressWarnings("rawtypes")
    private class InitRunner implements Function<RunnerWithTeardown[], Mono<RunnerWithTeardown[]>> {
      final int pos;
      final OutputManager out;
      final @Nullable MetricsContainer metrics;

      InitRunner(int pos, OutputManager out, @Nullable MetricsContainer metrics) {
        this.pos = pos;
        this.metrics = metrics;
        this.out = out;
      }

      @Override
      public Mono<RunnerWithTeardown[]> apply(RunnerWithTeardown[] runners) {
        // Last runner uses output, every other one fuses it's output directly into the next runner
        OutputManager out = pos + 1 < runners.length ? new FusedOut(runners[pos + 1]) : this.out;
        Mono<? extends RunnerWithTeardown<?, ?>> runner = factories.get(pos).create(out, metrics);
        return runner.doOnNext(r -> runners[pos] = r).thenReturn(runners);
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

    private static class FusedRunner<InT, T> implements RunnerWithTeardown<InT, T> {
      final RunnerWithTeardown<?, ?>[] runners;

      FusedRunner(RunnerWithTeardown<?, ?>[] runners) {
        this.runners = runners;
      }

      @Override
      public String toString() {
        return Arrays.toString(runners);
      }

      @Override
      public boolean isSDF() {
        return runners[runners.length - 1].isSDF();
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

  private static class MetricsRunner<InputT, OutputT>
      implements DoFnRunnerFactory.RunnerWithTeardown<InputT, OutputT> {
    private static final Closeable NOOP = () -> {};
    private final DoFnRunner<InputT, OutputT> runner;
    private final SimpleFactory<InputT, OutputT> factory;
    private final int id;
    private final @Nullable MetricsContainer metrics;

    MetricsRunner(
        DoFnRunner<InputT, OutputT> runner,
        SimpleFactory<InputT, OutputT> factory,
        @Nullable MetricsContainer metrics) {
      this.runner = runner;
      this.factory = factory;
      this.id = factory.nextId.incrementAndGet();
      this.metrics = metrics;
    }

    @Override
    public boolean isSDF() {
      return factory.isSDF;
    }

    @Override
    public String toString() {
      return "Runner[" + factory.transform.getName() + "," + id + "]";
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return runner.getFn();
    }

    private Closeable scopedMetricsContainer() {
      return metrics != null ? MetricsEnvironment.scopedMetricsContainer(metrics) : NOOP;
    }

    @Override
    public void startBundle() {
      try (Closeable ignored = scopedMetricsContainer()) {
        try {
          runner.startBundle();
        } catch (RuntimeException re) {
          teardown();
          throw re;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void processElement(final WindowedValue<InputT> elem) {
      try (Closeable ignored = scopedMetricsContainer()) {
        runner.processElement(elem);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <KeyT> void onTimer(
        final String timerId,
        final String timerFamilyId,
        KeyT key,
        final BoundedWindow window,
        final Instant timestamp,
        final Instant outputTimestamp,
        final TimeDomain timeDomain) {
      try (Closeable ignored = scopedMetricsContainer()) {
        runner.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void finishBundle() {
      try (Closeable ignored = scopedMetricsContainer()) {
        runner.finishBundle();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
      runner.onWindowExpiration(window, timestamp, key);
    }

    @Override
    public void teardown() {
      DoFnInvokers.invokerFor(getFn()).invokeTeardown();
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
