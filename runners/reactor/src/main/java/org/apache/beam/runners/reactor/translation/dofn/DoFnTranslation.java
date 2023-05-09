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
import static org.apache.beam.runners.reactor.LocalPipelineOptions.SDFMode.ASYNC;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory.RunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class DoFnTranslation<T1, T2> implements Translation<T1, T2>, Translation.CanFuse<T1, T2> {
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
      if (prevDoFn.factory.isSDF()) {
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
    // return flux.map(f -> simple(f, opts));
    throw new UnsupportedOperationException();
  }

  @Override
  public Flux<WindowedValue<T2>> simple(
      Flux<WindowedValue<T1>> fluxIn, int subscribers, LocalPipelineOptions opts) {
    final SinkOutputManager<T2> out = new SinkOutputManager<>(subscribers, factory.isSDF(), opts);
    return Flux.defer(() -> out.createOutput(fluxIn, factory, metrics));
  }

  /** OutputManager that emits output to a FluxSink. */
  private static class SinkOutputManager<T2> implements OutputManager {
    private final AtomicInteger counter = new AtomicInteger();
    private final int subscribers;
    private final @Nullable Scheduler scheduler;
    private Sinks.Many<WindowedValue<T2>> sink;

    SinkOutputManager(int subscribers, boolean isSdf, LocalPipelineOptions opts) {
      this.scheduler = isSdf && opts.getSDFMode() == ASYNC ? opts.getScheduler() : null;
      this.subscribers = Math.max(1, subscribers);
      Sinks.ManySpec many = Sinks.unsafe().many();
      this.sink = subscribers <= 1 ? many.unicast().onBackpressureBuffer() : many.replay().all();
    }

    <T1> Flux<WindowedValue<T2>> createOutput(
        Flux<WindowedValue<T1>> in,
        DoFnRunnerFactory<T1, T2> factory,
        @Nullable MetricsContainer metrics) {
      int count = counter.incrementAndGet();
      Flux<WindowedValue<T2>> internalOut = sink.asFlux();
      Flux<WindowedValue<T2>> out =
          count == 1
              ? Flux.usingWhen(
                  factory.create(this, metrics),
                  runner -> {
                    in.subscribe(new GroupSubscriber<>(runner, this));
                    return internalOut;
                  },
                  runner -> Mono.fromRunnable(runner::teardown))
              : internalOut; // just replay internal sink
      if (count > subscribers) {
        dispose();
      }

      if (scheduler == null) {
        return out;
      }
      Scheduler pinned = Schedulers.single(scheduler);
      return out.publishOn(pinned).doOnTerminate(pinned::dispose);
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      sink.emitNext((WindowedValue<T2>) out, FAIL_FAST); // TODO revisit
    }

    void complete() {
      sink.emitComplete(FAIL_FAST); // TODO Revisit
      if (counter.incrementAndGet() > subscribers) {
        dispose();
      }
    }

    void error(Throwable e) {
      sink.emitError(e, FAIL_FAST); // TODO Revisit
      if (counter.getAndDecrement() <= 0) {
        dispose();
      }
    }

    @SuppressWarnings("nullness")
    void dispose() {
      sink = null;
    }
  }

  private static class GroupSubscriber<T1, T2> extends BaseSubscriber<WindowedValue<T1>> {
    private static final Logger LOG = LoggerFactory.getLogger(GroupSubscriber.class);
    private final SinkOutputManager<T2> output;
    private final RunnerWithTeardown<T1, T2> runner;

    // Track thread used to call DoFn runner
    private final AtomicReference<@Nullable Thread> pinned = new AtomicReference<>();

    GroupSubscriber(RunnerWithTeardown<T1, T2> runner, SinkOutputManager<T2> output) {
      this.output = output;
      this.runner = runner;
    }

    @Override
    protected void hookOnSubscribe(Subscription subscription) {
      subscription.request(1);
    }

    @Override
    protected void hookOnNext(WindowedValue<T1> wv) {
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
      upstream().request(1);
    }

    @Override
    protected void hookOnComplete() {
      runner.finishBundle();
      output.complete();
    }

    @Override
    protected void hookOnError(Throwable e) {
      LOG.error("Received error signal", e);
      output.error(e);
    }
  }
}
