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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.reactor.translation.PipelineTranslator.Translation;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

class CombinePerKeyTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<KV<K, InT>>, PCollection<KV<K, OutT>>, Combine.PerKey<K, InT, OutT>> {

  @Override
  public void translate(Combine.PerKey<K, InT, OutT> transform, Context cxt) {
    CombineFn<InT, AccT, OutT> fn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    Scheduler scheduler = cxt.getScheduler();
    int parallelism = cxt.getOptions().getParallelism();
    cxt.translate(cxt.getOutput(), new TranslateCombine<>(fn, scheduler, parallelism));
  }

  // FIXME must also use rowconverter
  @SuppressWarnings("nullness")
  private static class TranslateCombine<K, InT, AccT, OutT>
      implements Translation<KV<K, InT>, KV<K, OutT>> {
    final CombineFn<InT, AccT, OutT> fn;
    final int parallelism;
    final Scheduler scheduler;

    TranslateCombine(CombineFn<InT, AccT, OutT> fn, Scheduler scheduler, int parallelism) {
      this.parallelism = parallelism;
      this.scheduler = scheduler;
      this.fn = fn;
    }

    @Override
    public Flux<? extends Flux<WindowedValue<KV<K, OutT>>>> apply(
        Flux<? extends Flux<WindowedValue<KV<K, InT>>>> flux) {
      return flux.subscribeOn(scheduler)
          .flatMap(group -> group.reduceWith(HashMap::new, this::add), parallelism)
          .reduce(this::merge)
          .flatMapIterable(Map::entrySet)
          .map(e -> valueInGlobalWindow(KV.of(e.getKey(), fn.extractOutput(e.getValue()))))
          .parallel(parallelism) // FIXME or skip this as there might be a reshuffle anyways?
          .runOn(scheduler)
          .groups();
    }

    private Map<K, AccT> add(Map<K, AccT> map, WindowedValue<KV<@NonNull K, InT>> wv) {
      KV<@NonNull K, InT> kv = wv.getValue();
      AccT acc = map.get(kv.getKey());
      if (acc == null) {
        acc = fn.createAccumulator();
        map.put(kv.getKey(), acc);
      }
      // FIXME assumes acc can be updated in place
      fn.addInput(acc, kv.getValue());
      return map;
    }

    private Map<K, AccT> merge(Map<K, AccT> map1, Map<K, AccT> map2) {
      if (map2.size() > map1.size()) {
        return merge(map2, map1);
      }
      map2.forEach((k, acc2) -> map1.compute(k, (ignore, acc1) -> merge(acc1, acc2)));
      return map1;
    }

    private AccT merge(@Nullable AccT acc1, AccT acc2) {
      return acc1 != null ? fn.mergeAccumulators(ImmutableList.of(acc1, acc2)) : acc2;
    }
  }
}
