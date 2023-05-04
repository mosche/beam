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
import static org.apache.beam.runners.reactor.LocalPipelineOptions.SDFMode.ASYNC_WITH_BACKPRESSURE;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory.RunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
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
  public <T0> boolean fuse(@Nullable Translation<T0, T1> prev, LocalPipelineOptions opts) {
    if (prev != null && prev instanceof DoFnTranslation) {
      DoFnTranslation<T1, T1> prevDoFn = (DoFnTranslation<T1, T1>) prev; // pretend input is T1
      if (prevDoFn.factory.isSDF() && !opts.isFuseSDF()) {
        return false;
      }
      factory = prevDoFn.factory.fuse(factory);
      return true;
    }
    return false;
  }

  @Override
  public Flux<? extends Flux<WindowedValue<T2>>> parallel(
      Flux<? extends Flux<WindowedValue<T1>>> flux, LocalPipelineOptions opts) {
    return flux.map(f -> simple(f, opts));
  }

  @Override
  public Flux<WindowedValue<T2>> simple(Flux<WindowedValue<T1>> flux, LocalPipelineOptions opts) {
    Flux<WindowedValue<T2>> outputFlux =
        Flux.create(sink -> runDoFn(flux, sink, opts)); // FIXME push/create?
    if (factory.isSDF() && opts.getSDFMode().async) {
      Scheduler pinned = Schedulers.single(Schedulers.parallel());
      return outputFlux.publishOn(pinned).doOnTerminate(pinned::dispose);
    }
    return outputFlux;
  }

  private void runDoFn(
      Flux<WindowedValue<T1>> flux, FluxSink<WindowedValue<T2>> sink, LocalPipelineOptions opts) {
    SinkOutput<T2> out =
        factory.isSDF() && opts.getSDFMode() == ASYNC_WITH_BACKPRESSURE
            ? new BackpressuringSinkOutput<>(sink)
            : new SinkOutput<>(sink);
    Mono<RunnerWithTeardown<T1, T2>> mRunner = factory.create(out, metrics);
    if (LOG.isDebugEnabled()) {
      mRunner = mRunner.doOnNext(runner -> LOG.debug("{}: created on {}", runner, currentThread()));
    }
    mRunner.subscribe(runner -> flux.subscribe(new GroupSubscriber<>(runner, out)), sink::error);
  }

  /**
   * OutputManager that tracks inflight elements to backpressure demand from upstream. This is only
   * necessary when publishing async.
   */
  private static class BackpressuringSinkOutput<T2> extends SinkOutput<T2> implements LongConsumer {
    private static final int MAX_INFLIGHT = 10000;
    final AtomicLong inflight = new AtomicLong();
    final AtomicBoolean rejected = new AtomicBoolean(false);
    @MonotonicNonNull Subscription subscription = null;

    @SuppressWarnings("argument")
    BackpressuringSinkOutput(FluxSink<WindowedValue<T2>> sink) {
      super(sink);
      sink.onRequest(this);
    }

    @Override
    void onSubscribe(Subscription subscription) {
      this.subscription = subscription;
      super.onSubscribe(subscription);
    }

    @Override
    boolean permitRequest() {
      if (inflight.get() < MAX_INFLIGHT) {
        return true;
      }
      rejected.set(true);
      return false;
    }

    @Override
    public void accept(long demand) {
      if (inflight.addAndGet(-demand) < MAX_INFLIGHT
          && subscription != null
          && rejected.compareAndSet(true, false)) {
        subscription.request(1);
      }
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      inflight.incrementAndGet();
      super.output(tag, out);
    }
  }

  /** OutputManager that emits output to a FluxSink. */
  private static class SinkOutput<T2> implements OutputManager {
    final FluxSink<WindowedValue<T2>> sink;

    SinkOutput(FluxSink<WindowedValue<T2>> sink) {
      this.sink = sink;
    }

    boolean permitRequest() {
      return true;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      sink.next((WindowedValue<T2>) out);
    }

    void onSubscribe(Subscription subscription) {
      sink.onCancel(subscription::cancel);
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
      output.onSubscribe(s);
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
        if (output.permitRequest()) {
          subscription.request(1);
        }
      } catch (RuntimeException e) {
        subscription.cancel();
        onError(e);
      }
    }

    @Override
    public void onComplete() {
      try {
        runner.finishBundle();
        output.sink.complete();
        runner.teardown();
      } catch (RuntimeException e) {
        onError(e);
      }
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("Received error signal", e);
      output.sink.error(e);
      runner.teardown();
    }
  }
}
