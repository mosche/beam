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
package org.apache.beam.runners.reactor.translation.batch;

import static java.util.function.Function.identity;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.runners.reactor.translation.PipelineTranslator.Translation;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

class ReshuffleTranslatorBatch<K, V>
    extends TransformTranslator<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> {

  @Override
  protected void translate(Reshuffle<K, V> transform, Context cxt) throws IOException {
    Scheduler scheduler = cxt.getScheduler();
    int parallelism = cxt.getOptions().getParallelism();
    cxt.translate(cxt.getOutput(), Dispatcher.regroup(parallelism, scheduler));
  }

  static class ViaRandomKey<V>
      extends TransformTranslator<PCollection<V>, PCollection<V>, Reshuffle.ViaRandomKey<V>> {

    @Override
    protected void translate(Reshuffle.ViaRandomKey<V> transform, Context cxt) throws IOException {
      int parallelism = cxt.getOptions().getParallelism();
      Scheduler scheduler = cxt.getScheduler();
      cxt.<V, V>translate(
          cxt.getOutput(),
          f ->
              f.subscribeOn(scheduler)
                  .flatMap(identity(), parallelism)
                  .parallel(parallelism)
                  .groups());
    }
  }

  @SuppressWarnings("rawtypes")
  private static class Dispatcher<K, V> implements Subscriber<WindowedValue<KV<K, V>>> {
    private final Scheduler scheduler;
    private final FluxSink[] sinks;
    private Subscription subscription;

    @SuppressWarnings({"initialization.fields.uninitialized"})
    Dispatcher(int parallelism, Scheduler scheduler) {
      this.scheduler = scheduler;
      this.sinks = new FluxSink[parallelism];
    }

    static <K, V> Translation<KV<K, V>, KV<K, V>> regroup(int parallelism, Scheduler scheduler) {
      return f ->
          f.flatMap(identity(), parallelism)
              .transform(
                  flattened -> {
                    Dispatcher<K, V> dispatcher = new Dispatcher<>(parallelism, scheduler);
                    flattened.subscribe(dispatcher);
                    return dispatcher.groupedFlux();
                  });
    }

    private Flux<Flux<WindowedValue<KV<K, V>>>> groupedFlux() {
      AtomicInteger pendingSubscriptions = new AtomicInteger(sinks.length);
      Flux[] groups = new Flux[sinks.length];
      for (int idx = 0; idx < sinks.length; idx++) {
        groups[idx] =
            Flux.create(new SubscribeOnce(idx, pendingSubscriptions)).subscribeOn(scheduler);
      }
      return Flux.just(groups);
    }

    @Override
    public void onSubscribe(Subscription s) {
      subscription = s;
    }

    // FIXME support arrays / rows as key
    int sinkIdx(WindowedValue<KV<K, V>> wv) {
      K key = wv.getValue() != null ? wv.getValue().getKey() : null;
      if (key == null) {
        return 0;
      }
      int rawMod = key.hashCode() % sinks.length;
      return rawMod + (rawMod < 0 ? sinks.length : 0);
    }

    @Override
    public void onNext(WindowedValue<KV<K, V>> wv) {
      FluxSink sink = sinks[sinkIdx(wv)];
      sink.next(wv);
      subscription.request(1);
    }

    @Override
    public void onError(Throwable t) {
      for (int i = 0; i < sinks.length; i++) {
        sinks[i].error(t);
      }
    }

    @Override
    public void onComplete() {
      for (int i = 0; i < sinks.length; i++) {
        sinks[i].complete();
      }
    }

    class SubscribeOnce<T> implements Consumer<FluxSink<T>> {
      final AtomicBoolean created = new AtomicBoolean(false);
      final AtomicInteger pendingSinks;
      final int idx;

      SubscribeOnce(int idx, AtomicInteger pendingSinks) {
        this.idx = idx;
        this.pendingSinks = pendingSinks;
      }

      @Override
      public void accept(FluxSink<T> sink) {
        if (created.compareAndSet(false, true)) {
          sinks[idx] = sink;
          // dispose sources if a sink is cancelled
          sink.onCancel(subscription::cancel);
          if (pendingSinks.decrementAndGet() == 0) {
            // once all sinks are connected, signal demand
            subscription.request(sinks.length);
          }
        } else {
          throw new UnsupportedOperationException("Only one subscriber allowed");
        }
      }
    }
  }
}
