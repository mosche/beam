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

import static java.lang.Thread.currentThread;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory.RunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DoFnTranslation<T1, T2> implements Translation<T1, T2>, Translation.CanFuse<T1, T2> {
  private static final Logger LOG = LoggerFactory.getLogger(DoFnTranslation.class);
  private final @Nullable MetricsContainer metrics;
  private DoFnRunnerFactory<T1, T2> factory;

  public DoFnTranslation(DoFnRunnerFactory<T1, T2> factory, @Nullable MetricsContainer metrics) {
    this.metrics = metrics;
    this.factory = factory;
  }

  @Override
  public <T0> boolean fuse(@Nullable Translation<T0, T1> prev) {
    if (prev != null && prev instanceof DoFnTranslation) {
      DoFnTranslation<T1, T1> prevDoFn = (DoFnTranslation<T1, T1>) prev; // pretend input is T1
      if (!prevDoFn.factory.mayFuse()) {
        return false;
      }
      factory = prevDoFn.factory.fuse(factory);
      return true;
    }
    return false;
  }

  @Override
  public Flux<WindowedValue<T2>> simple(Flux<WindowedValue<T1>> flux) {
    SinkOutput<T2> out = new SinkOutput<>(); // FIXME Use FluxSink instead?
    Mono<RunnerWithTeardown<T1, T2>> mRunner = factory.create(out, metrics);
    if (LOG.isDebugEnabled()) {
      mRunner = mRunner.doOnNext(runner -> LOG.debug("{}: created on {}", runner, currentThread()));
    }
    return mRunner.flatMapMany(
        runner -> {
          flux.subscribe(new GroupSubscriber<>(runner, out));
          Flux<WindowedValue<T2>> outputFlux = out.sink.asFlux();
          if (factory.mayFuse()) {
            return outputFlux;
          } else {
            Scheduler pinned = Schedulers.single(Schedulers.parallel());
            if (LOG.isDebugEnabled()) {
              pinned.schedule(() -> LOG.debug("{}: publishing SDF on {}", runner, currentThread()));
            }
            return outputFlux.publishOn(pinned).doOnTerminate(pinned::dispose);
          }
        });
  }

  @Override
  public Flux<? extends Flux<WindowedValue<T2>>> parallel(
      Flux<? extends Flux<WindowedValue<T1>>> flux, int parallelism, Scheduler scheduler) {
    return flux.map(this::simple);
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

    // Track thread used to call DoFn runner
    private final AtomicReference<@Nullable Thread> pinned = new AtomicReference<>();

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
        Thread current = currentThread();
        if (pinned.compareAndSet(null, current)) {
          LOG.debug("{}: startBundle [{}]", runner, current);
          runner.startBundle();
        } else if (current != pinned.get()) {
          LOG.warn("{}: processElement called on {}, expected {}", runner, current, pinned.get());
          pinned.lazySet(current); // update pinned lazily
        }

        LOG.trace("{}: processElement {} [{}]", runner, wv.getValue(), current);
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
