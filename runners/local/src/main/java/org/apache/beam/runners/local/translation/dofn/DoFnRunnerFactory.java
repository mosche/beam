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
package org.apache.beam.runners.local.translation.dofn;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.beam.runners.core.construction.ParDoTranslation.getSchemaInformation;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.local.LocalPipelineOptions;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DoFnRunnerFactory<InT, T> {
  interface DoFnRunnerWithTeardown<InT, T> extends DoFnRunner<InT, T> {
    void teardown();
  }

  /**
   * Creates a runner that is ready to process elements.
   *
   * <p>Both, {@link org.apache.beam.sdk.transforms.reflect.DoFnInvoker#invokeSetup setup} and
   * {@link DoFnRunner#startBundle()} are already invoked by the factory.
   */
  abstract DoFnRunnerWithTeardown<InT, T> create(
      LocalPipelineOptions opts, MetricsContainer metrics, OutputManager output);

  /**
   * Fuses the factory for the following {@link DoFnRunner} into a single factory that processes
   * both DoFns in a single step.
   */
  abstract <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next);

  abstract boolean requiresCollect();

  public static <InT, T> DoFnRunnerFactory<InT, T> simple(
      AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
      PCollection<InT> input,
      SideInputReader sideInputReader) {
    return new SimpleRunnerFactory<>(appliedPT, input, sideInputReader);
  }

  private static class SimpleRunnerFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleRunnerFactory.class);

    private static final PTransformMatcher SPLITTABLE_MATCHER =
        PTransformMatchers.parDoWithFnType(SplittableParDoNaiveBounded.NaiveProcessFn.class);

    final AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT;
    final PCollection<InT> input;
    final SideInputReader sideInputs;

    final AtomicInteger count = new AtomicInteger(0);

    private SimpleRunnerFactory(
        AppliedPTransform<PCollection<? extends InT>, ?, ParDo.MultiOutput<InT, T>> appliedPT,
        PCollection<InT> input,
        SideInputReader sideInputs) {
      this.appliedPT = appliedPT;
      this.input = input;
      this.sideInputs = sideInputs;
    }

    /** Collect is required for splittable ParDos so processing can be parallelized. */
    @Override
    boolean requiresCollect() {
      return SPLITTABLE_MATCHER.matches(appliedPT);
    }

    @Override
    DoFnRunnerWithTeardown<InT, T> create(
        LocalPipelineOptions opts, MetricsContainer metrics, OutputManager output) {
      LOG.warn("Creating runner {}: {}", count.incrementAndGet(), appliedPT.getFullName());

      ParDo.MultiOutput<InT, T> tf = appliedPT.getTransform();
      List<TupleTag<?>> additionalOuts = tf.getAdditionalOutputTags().getAll();
      TupleTag<T> mainOut = tf.getMainOutputTag();
      DoFnRunner<InT, T> simpleRunner =
          DoFnRunners.simpleRunner(
              opts,
              tf.getFn(),
              sideInputs,
              additionalOuts.isEmpty() ? output : new FilteredOutput(output, mainOut),
              mainOut,
              additionalOuts,
              NoOpStepContext.INSTANCE,
              input.getCoder(),
              EMPTY_MAP, // no coders used
              WindowingStrategy.globalDefault(),
              getSchemaInformation(appliedPT),
              ImmutableMap.of());

      DoFnRunnerWithTeardown<InT, T> runner =
          opts.isMetricsEnabled()
              ? new DoFnRunnerWithMetrics<>(simpleRunner, metrics)
              : new DoFnRunnerDelegate<>(simpleRunner);

      // Invoke setup and then startBundle before returning the runner
      DoFnInvokers.tryInvokeSetupFor(tf.getFn(), opts);
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
      return new FusedRunnerFactory<>(Lists.newArrayList(this, next));
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
  private static class FusedRunnerFactory<InT, T> extends DoFnRunnerFactory<InT, T> {
    private final List<DoFnRunnerFactory<?, ?>> factories;

    FusedRunnerFactory(List<DoFnRunnerFactory<?, ?>> factories) {
      this.factories = factories;
    }

    @Override
    DoFnRunnerWithTeardown<InT, T> create(
        LocalPipelineOptions opts, MetricsContainer metrics, OutputManager output) {
      return new FusedRunner<>(opts, metrics, output, factories);
    }

    @Override
    <T2> DoFnRunnerFactory<InT, T2> fuse(DoFnRunnerFactory<T, T2> next) {
      factories.add(next);
      return (DoFnRunnerFactory<InT, T2>) this;
    }

    @Override
    boolean requiresCollect() {
      return false;
    }

    private static class FusedRunner<InT, T> implements DoFnRunnerWithTeardown<InT, T> {
      final DoFnRunnerWithTeardown<?, ?>[] runners;

      FusedRunner(
          LocalPipelineOptions opts,
          MetricsContainer metrics,
          OutputManager output,
          List<DoFnRunnerFactory<?, ?>> factories) {
        runners = new DoFnRunnerWithTeardown<?, ?>[factories.size()];
        runners[runners.length - 1] =
            factories.get(runners.length - 1).create(opts, metrics, output);
        for (int i = runners.length - 2; i >= 0; i--) {
          runners[i] = factories.get(i).create(opts, metrics, new FusedOutput(runners[i + 1]));
        }
      }

      /** {@link OutputManager} that forwards output directly to the next runner. */
      private static class FusedOutput implements OutputManager {
        final DoFnRunnerWithTeardown<?, ?> runner;

        FusedOutput(DoFnRunnerWithTeardown<?, ?> runner) {
          this.runner = runner;
        }

        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          runner.processElement((WindowedValue) output);
        }
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

  private static class DoFnRunnerDelegate<InT, T> implements DoFnRunnerWithTeardown<InT, T> {
    private final DoFnRunner<InT, T> runner;

    DoFnRunnerDelegate(DoFnRunner<InT, T> runner) {
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

    @Override
    public void teardown() {
      DoFnInvokers.invokerFor(runner.getFn()).invokeTeardown();
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
