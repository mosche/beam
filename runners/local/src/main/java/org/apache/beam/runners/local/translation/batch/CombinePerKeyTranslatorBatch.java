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

import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTask;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;

import static avro.shaded.com.google.common.collect.Collections2.transform;
import static java.util.Collections.EMPTY_LIST;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;

class CombinePerKeyTranslatorBatch<K, InT, AccT, OutT>
    extends TransformTranslator<
        PCollection<KV<K, InT>>, PCollection<KV<K, OutT>>, Combine.PerKey<K, InT, OutT>> {

  @Override
  public void translate(Combine.PerKey<K, InT, OutT> transform, Context cxt) {
    CombineFn<InT, AccT, OutT> combineFn = (CombineFn<InT, AccT, OutT>) transform.getFn();
    Dataset<KV<K, InT>> dataset = cxt.getDataset(cxt.getInput());
    cxt.putDataset(
        cxt.getOutput(), dataset.transform(splits -> new CombineTask<>(splits, combineFn)));
  }

  private static class CombineTask<K, InT, AccT, OutT>
      extends TransformTask<
          WindowedValue<KV<K, InT>>,
          Map<K, AccT>,
          Collection<WindowedValue<KV<K, OutT>>>,
          CombineTask<K, InT, AccT, OutT>> {
    final CombineFn<InT, AccT, OutT> fn;

    CombineTask(
        List<Spliterator<WindowedValue<KV<K, InT>>>> splits, CombineFn<InT, AccT, OutT> fn) {
      this(null, null, splits, 0, splits.size(), fn);
    }

    private CombineTask(
        @Nullable CombineTask<K, InT, AccT, OutT> parent,
        @Nullable CombineTask<K, InT, AccT, OutT> next,
        List<Spliterator<WindowedValue<KV<K, InT>>>> splits,
        int lo,
        int hi,
        CombineFn<InT, AccT, OutT> fn) {
      super(parent, next, splits, lo, hi);
      this.fn = fn;
    }

    @Override
    protected CombineTask<K, InT, AccT, OutT> subTask(
        CombineTask<K, InT, AccT, OutT> parent,
        CombineTask<K, InT, AccT, OutT> next,
        List<Spliterator<WindowedValue<KV<K, InT>>>> splits,
        int lo,
        int hi) {
      return new CombineTask<>(parent, next, splits, lo, hi, fn);
    }

    @Override
    protected Map<K, AccT> add(@Nullable Map<K, AccT> accMap, WindowedValue<KV<K, InT>> wv) {
      if (accMap == null) {
        accMap = new HashMap<>();
      }
      KV<K, InT> kv = wv.getValue();
      accMap.compute(kv.getKey(), (k, acc) -> addInput(acc, kv.getValue()));
      return accMap;
    }

    private AccT addInput(@Nullable AccT acc, InT v) {
      return fn.addInput(acc != null ? acc : fn.createAccumulator(), v);
    }

    @Override
    protected Collection<WindowedValue<KV<K, OutT>>> getOutput(@Nullable Map<K, AccT> map) {
      return map != null
          ? transform(
              map.entrySet(),
              e -> valueInGlobalWindow(KV.of(e.getKey(), fn.extractOutput(e.getValue()))))
          : EMPTY_LIST;
    }

    @Override
    protected Map<K, AccT> merge(Map<K, AccT> left, Map<K, AccT> right) {
      for (Entry<K, AccT> e : right.entrySet()) {
        left.compute(e.getKey(), (k, v) -> fn.mergeAccumulators(ImmutableList.of(v, e.getValue())));
      }
      return left;
    }
  }
}
