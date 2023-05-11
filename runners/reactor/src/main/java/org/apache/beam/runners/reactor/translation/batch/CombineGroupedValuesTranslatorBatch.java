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

import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import reactor.core.publisher.Flux;

class CombineGroupedValuesTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<? extends KV<K, ? extends Iterable<InT>>>,
        PCollection<KV<K, OutT>>,
        GroupedValues<K, InT, OutT>> {

  @Override
  protected void translate(
      Context<
              PCollection<? extends KV<K, ? extends Iterable<InT>>>,
              PCollection<KV<K, OutT>>,
              GroupedValues<K, InT, OutT>>
          cxt) {
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) cxt.getTransform().getFn();
    cxt.translate(cxt.getOutput(), new TranslateCombineValues<>(combineFn));
  }

  @Override
  public boolean canTranslate(GroupedValues<K, InT, OutT> transform) {
    return !(transform.getFn() instanceof CombineWithContext);
  }

  // FIXME Fuse with group by key
  private static class TranslateCombineValues<K, InT, AccT, OutT>
      extends Translation.BasicTranslation<KV<K, Iterable<InT>>, KV<K, OutT>> {
    final CombineFn<InT, AccT, OutT> fn;

    TranslateCombineValues(CombineFn<InT, AccT, OutT> fn) {
      this.fn = fn;
    }

    private WindowedValue<KV<K, OutT>> reduce(WindowedValue<KV<K, Iterable<InT>>> wv) {
      KV<K, Iterable<InT>> kv = wv.getValue();
      AccT acc = fn.createAccumulator();
      // FIXME this assumes acc is mutated in place
      kv.getValue().forEach(in -> fn.addInput(acc, in));
      return WindowedValue.valueInGlobalWindow(KV.of(kv.getKey(), fn.extractOutput(acc)));
    }

    @Override
    public Flux<WindowedValue<KV<K, OutT>>> simple(
        Flux<WindowedValue<KV<K, Iterable<InT>>>> flux, ReactorOptions opts) {
      return flux.map(this::reduce);
    }
  }
}
