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
import static org.apache.beam.runners.reactor.ReactorOptions.SDFMode.ASYNC;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory.RunnerWithTeardown;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
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
  public Flux<WindowedValue<T2>> simple(
      Flux<WindowedValue<T1>> fluxIn, int subscribers, ReactorOptions opts) {
    final SingleDoFnSink<T2> out = new SingleDoFnSink<>(subscribers, factory.isSDF(), opts);
    return Flux.defer(() -> out.getOrSubscribe(fluxIn, out.sink.asFlux(), factory, metrics));
  }

  @SuppressWarnings("argument") // null keys not allowed here
  @Override
  public Map<TupleTag<?>, Flux<WindowedValue<T2>>> simpleTagged(
      Flux<WindowedValue<T1>> fluxIn, Map<TupleTag<?>, Integer> subscribers, ReactorOptions opts) {
    final TaggedDoFnSink<T2> out = new TaggedDoFnSink<>(subscribers, factory.isSDF(), opts);
    return Maps.asMap(
        subscribers.keySet(),
        tag -> Flux.defer(() -> out.getOrSubscribe(tag, fluxIn, factory, metrics)));
  }

  private abstract static class DoFnSink implements OutputManager {
    private final AtomicBoolean isSubscribed = new AtomicBoolean();
    private final @Nullable Scheduler scheduler;

    private DoFnSink(boolean isSdf, ReactorOptions opts) {
      this.scheduler = isSdf && opts.getSDFMode() == ASYNC ? opts.getScheduler() : null;
    }

    protected <T1, T2> Flux<WindowedValue<T2>> getOrSubscribe(
        Flux<WindowedValue<T1>> in,
        Flux<WindowedValue<T2>> out,
        DoFnRunnerFactory<T1, T2> factory,
        @Nullable MetricsContainer metrics) {
      Flux<WindowedValue<T2>> subscribedOut =
          isSubscribed.compareAndSet(false, true)
              ? Flux.usingWhen(
                  factory.create(this, metrics),
                  runner -> {
                    in.subscribe(new GroupSubscriber<>(runner, this));
                    return out;
                  },
                  runner -> Mono.fromRunnable(runner::teardown))
              : out;
      if (scheduler == null) {
        return subscribedOut;
      }
      Scheduler pinned = Schedulers.single(scheduler);
      return subscribedOut.publishOn(pinned).doOnTerminate(pinned::dispose);
    }

    static <T2> Sinks.Many<WindowedValue<T2>> createSink(int subscribers) {
      Sinks.ManySpec many = Sinks.unsafe().many();
      // TODO enable memory optimized mode via options using publish with refcount
      return subscribers <= 1 ? many.unicast().onBackpressureBuffer() : many.replay().all();
    }

    abstract void complete();

    abstract void error(Throwable e);
  }

  /** OutputManager that emits DoFn output to a FluxSink. */
  private static class SingleDoFnSink<T2> extends DoFnSink {
    private Sinks.Many<WindowedValue<T2>> sink;

    SingleDoFnSink(int subscribers, boolean isSdf, ReactorOptions opts) {
      super(isSdf, opts);
      this.sink = createSink(subscribers);
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      sink.emitNext((WindowedValue<T2>) out, FAIL_FAST); // TODO revisit
    }

    @Override
    void complete() {
      sink.emitComplete(FAIL_FAST); // TODO Revisit
    }

    @Override
    void error(Throwable e) {
      sink.emitError(e, FAIL_FAST); // TODO Revisit
    }
  }

  private static class TaggedDoFnSink<T2> extends DoFnSink {
    private final Map<TupleTag<?>, Sinks.Many<WindowedValue<T2>>> sinks;

    TaggedDoFnSink(
        Map<TupleTag<?>, Integer> subscribersPerOutput, boolean isSdf, ReactorOptions opts) {
      super(isSdf, opts);
      this.sinks = Maps.newHashMapWithExpectedSize(subscribersPerOutput.size());
      subscribersPerOutput.forEach((tag, subscribers) -> sinks.put(tag, createSink(subscribers)));
    }

    <T1> Flux<WindowedValue<T2>> getOrSubscribe(
        TupleTag<?> tag,
        Flux<WindowedValue<T1>> in,
        DoFnRunnerFactory<T1, T2> factory,
        @Nullable MetricsContainer metrics) {
      return getOrSubscribe(in, checkStateNotNull(sinks.get(tag)).asFlux(), factory, metrics);
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> out) {
      checkStateNotNull(sinks.get(tag))
          .emitNext((WindowedValue<T2>) out, FAIL_FAST); // TODO revisit
    }

    @Override
    void complete() {
      sinks.forEach((t, sink) -> sink.emitComplete(FAIL_FAST)); // TODO Revisit
    }

    @Override
    void error(Throwable e) {
      sinks.forEach((t, sink) -> sink.emitError(e, FAIL_FAST)); // TODO Revisit
    }
  }

  private static class GroupSubscriber<T1, T2> extends BaseSubscriber<WindowedValue<T1>> {
    private static final Logger LOG = LoggerFactory.getLogger(GroupSubscriber.class);
    private final DoFnSink output;
    private final RunnerWithTeardown<T1, T2> runner;

    // Track thread used to call DoFn runner
    private final AtomicReference<@Nullable Thread> pinned = new AtomicReference<>();

    GroupSubscriber(RunnerWithTeardown<T1, T2> runner, DoFnSink output) {
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
