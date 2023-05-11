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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

class ReshuffleTranslatorBatch<K, V>
    extends TransformTranslator<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> {

  @Override
  protected void translate(
      Context<PCollection<KV<K, V>>, PCollection<KV<K, V>>, Reshuffle<K, V>> cxt) {
    cxt.translate(cxt.getOutput(), new ReshuffleTranslation<>());
  }

  static class ViaRandomKey<V>
      extends TransformTranslator<PCollection<V>, PCollection<V>, Reshuffle.ViaRandomKey<V>> {

    @Override
    protected void translate(
        Context<PCollection<V>, PCollection<V>, Reshuffle.ViaRandomKey<V>> cxt) {
      cxt.<V, V>translate(cxt.getOutput(), new RandomReshuffleTranslation<>());
    }
  }

  private static class RandomReshuffleTranslation<V> extends Translation.BasicTranslation<V, V> {

    @Override
    public Flux<WindowedValue<V>> simple(Flux<WindowedValue<V>> flux, ReactorOptions opts) {
      return flux;
    }

    @Override
    public Flux<? extends Flux<WindowedValue<V>>> parallel(
        Flux<? extends Flux<WindowedValue<V>>> flux, ReactorOptions opts) {
      return flux.subscribeOn(opts.getScheduler())
          .flatMap(Function.identity(), opts.getParallelism())
          .parallel(opts.getParallelism())
          .groups();
    }
  }

  private static class ReshuffleTranslation<K, V>
      extends Translation.BasicTranslation<KV<K, V>, KV<K, V>> {
    @Override
    public Flux<WindowedValue<KV<K, V>>> simple(
        Flux<WindowedValue<KV<K, V>>> flux, ReactorOptions opts) {
      return flux; // noop
    }

    @Override
    public Flux<? extends Flux<WindowedValue<KV<K, V>>>> parallel(
        Flux<? extends Flux<WindowedValue<KV<K, V>>>> flux, ReactorOptions opts) {
      return flux.flatMap(Function.identity(), opts.getParallelism())
          .transform(
              flattened -> {
                Dispatcher<K, V> dispatcher =
                    new Dispatcher<>(opts.getParallelism(), opts.getScheduler());
                flattened.subscribe(dispatcher);
                return dispatcher.groupedFlux();
              });
    }

    @SuppressWarnings("rawtypes")
    private static class Dispatcher<K, V> extends BaseSubscriber<WindowedValue<KV<K, V>>> {
      private final Scheduler scheduler;
      private final FluxSink[] sinks;

      Dispatcher(int parallelism, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.sinks = new FluxSink[parallelism];
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
      protected void hookOnSubscribe(Subscription subscription) {}

      @Override
      protected void hookOnNext(WindowedValue<KV<K, V>> wv) {
        sinks[sinkIdx(wv)].next(wv);
        upstream().request(1);
      }

      @Override
      protected void hookOnComplete() {
        for (int i = 0; i < sinks.length; i++) {
          sinks[i].complete();
        }
      }

      @Override
      protected void hookOnError(Throwable t) {
        for (int i = 0; i < sinks.length; i++) {
          sinks[i].error(t);
        }
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
            sink.onCancel(() -> upstream().cancel());
            if (pendingSinks.decrementAndGet() == 0) {
              // once all sinks are connected, signal demand
              upstream().request(sinks.length);
            }
          } else {
            throw new UnsupportedOperationException("Only one subscriber allowed");
          }
        }
      }
    }
  }
}
