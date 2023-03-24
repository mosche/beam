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

import java.io.IOException;
import org.apache.beam.runners.reactor.translation.PipelineTranslator.Translation;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import reactor.core.publisher.Flux;

class CombineGroupedValuesTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<? extends KV<K, ? extends Iterable<InT>>>,
        PCollection<KV<K, OutT>>,
        Combine.GroupedValues<K, InT, OutT>> {

  @Override
  protected void translate(Combine.GroupedValues<K, InT, OutT> transform, Context cxt)
      throws IOException {
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    cxt.translate(cxt.getOutput(), new TranslateCombineValues<>(combineFn));
  }

  @Override
  public boolean canTranslate(Combine.GroupedValues<K, InT, OutT> transform) {
    return !(transform.getFn() instanceof CombineWithContext);
  }

  // FIXME Fuse with group by key
  private static class TranslateCombineValues<K, InT, AccT, OutT>
      implements Translation<KV<K, Iterable<InT>>, KV<K, OutT>> {
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
    public Flux<Flux<WindowedValue<KV<K, OutT>>>> apply(
        Flux<? extends Flux<WindowedValue<KV<K, Iterable<InT>>>>> flux) {
      return flux.map(gf -> gf.map(this::reduce));
    }
  }
}
