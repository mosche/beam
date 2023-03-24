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

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.runners.reactor.translation.PipelineTranslator.Translation;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory.RunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;

public class DoFnTranslation<T1, T2> implements Translation<T1, T2>, Translation.CanFuse<T1, T2> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnTranslation.class);
  private final LocalPipelineOptions opts;
  private final MetricsContainer metrics;
  private final Scheduler scheduler;
  private DoFnRunnerFactory<T1, T2> factory;

  public DoFnTranslation(
      LocalPipelineOptions opts,
      MetricsContainer metrics,
      Scheduler scheduler,
      DoFnRunnerFactory<T1, T2> factory) {
    this.opts = opts;
    this.metrics = metrics;
    this.scheduler = scheduler;
    this.factory = factory;
  }

  @Override
  public <T0> boolean fuse(@Nullable Translation<T0, T1> prev) {
    if (opts.isFuseDoFnsEnabled() && prev != null && prev instanceof DoFnTranslation) {
      DoFnTranslation<T1, T1> prevDoFn = (DoFnTranslation<T1, T1>) prev; // pretend input is T1
      factory = prevDoFn.factory.fuse(factory);
      return true;
    }
    return false;
  }

  @Override
  public Flux<? extends Flux<WindowedValue<T2>>> apply(
      Flux<? extends Flux<WindowedValue<T1>>> flux) {
    return flux.map(
        group -> {
          SinkOutput<T2> out = new SinkOutput<>(); // FIXME Use FluxSink instead?
          return factory
              .create(opts, metrics, out)
              .subscribeOn(scheduler)
              .flatMapMany(
                  runner -> {
                    group.subscribe(new GroupSubscriber<>(runner, out));
                    return out.sink.asFlux().subscribeOn(scheduler);
                  });
        });
  }

  private static class SinkOutput<T2> implements OutputManager {
    final Many<WindowedValue<T2>> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
    volatile Disposable disposable = () -> {};

    void onError(Disposable disposable) {
      this.disposable = disposable;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      sink.emitNext(
          (WindowedValue<T2>) out,
          (signal, result) -> {
            disposable.dispose();
            return false;
          });
    }
  }

  private static class GroupSubscriber<T1, T2> implements Subscriber<WindowedValue<T1>> {
    private static final Logger LOG = LoggerFactory.getLogger(GroupSubscriber.class);
    private final SinkOutput<T2> output;
    private final RunnerWithTeardown<T1, T2> runner;
    private Subscription subscription;

    @SuppressWarnings({"argument", "initialization.fields.uninitialized"})
    GroupSubscriber(RunnerWithTeardown<T1, T2> runner, SinkOutput<T2> output) {
      this.output = output;
      this.runner = runner;
    }

    @Override
    public void onSubscribe(Subscription s) {
      subscription = s;
      output.onError(() -> subscription.cancel());
      subscription.request(1);
    }

    @Override
    public void onNext(WindowedValue<T1> wv) {
      try {
        runner.processElement(wv);
        // FIXME better manage demand based on downstream requests
        subscription.request(1);
      } catch (RuntimeException e) {
        subscription.cancel();
        onError(e);
      }
    }

    @Override
    public void onComplete() {
      try {
        runner.finishBundle();
        output.sink.emitComplete(FAIL_FAST);
        runner.teardown();
      } catch (RuntimeException e) {
        onError(e);
      }
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("Received error signal", e);
      output.sink.emitError(e, FAIL_FAST);
      runner.teardown();
    }
  }
}
