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
package org.apache.beam.runners.local.translation.batch;

import java.io.IOException;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.Dataset.MapFn;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class CombineGroupedValuesTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<? extends KV<K, ? extends Iterable<InT>>>,
        PCollection<KV<K, OutT>>,
        Combine.GroupedValues<K, InT, OutT>> {

  @Override
  protected void translate(Combine.GroupedValues<K, InT, OutT> transform, Context cxt)
      throws IOException {

    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    MapFn<KV<K, Iterable<InT>>, KV<K, OutT>> reduceValuesFn =
        wv -> {
          KV<K, Iterable<InT>> kv = wv.getValue();
          AccT acc = null;
          for (InT in : kv.getValue()) {
            acc = combineFn.addInput(acc != null ? acc : combineFn.createAccumulator(), in);
          }
          OutT value = acc != null ? combineFn.extractOutput(acc) : combineFn.defaultValue();
          return wv.withValue(KV.of(kv.getKey(), value));
        };

    Dataset<KV<K, Iterable<InT>>> dataset =
        (Dataset<KV<K, Iterable<InT>>>) cxt.requireDataset(cxt.getInput());
    cxt.provideDataset(cxt.getOutput(), dataset.map(cxt.fullName(), reduceValuesFn));
  }

  @Override
  public boolean canTranslate(Combine.GroupedValues<K, InT, OutT> transform) {
    return !(transform.getFn() instanceof CombineWithContext);
  }
}
