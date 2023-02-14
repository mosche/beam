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
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Collections2.transform;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.concat;

class GroupByKeyTranslatorBatch<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  @Override
  public void translate(GroupByKey<K, V> transform, Context cxt) {
    Dataset<KV<K, V>> dataset = cxt.getDataset(cxt.getInput());
    cxt.putDataset(cxt.getOutput(), dataset.transform(splits -> new GroupByKeyTask<>(splits)));
  }

  private static class GroupByKeyTask<K, V>
      extends TransformTask<
          WindowedValue<KV<K, V>>,
          Map<K, Iterable<V>>,
          Collection<WindowedValue<KV<K, Iterable<V>>>>,
          GroupByKeyTask<K, V>> {

    GroupByKeyTask(List<Spliterator<WindowedValue<KV<K, V>>>> splits) {
      this(null, null, splits, 0, splits.size());
    }

    private GroupByKeyTask(
        @Nullable GroupByKeyTask<K, V> parent,
        @Nullable GroupByKeyTask<K, V> next,
        List<Spliterator<WindowedValue<KV<K, V>>>> splits,
        int lo,
        int hi) {
      super(parent, next, splits, lo, hi);
    }

    @Override
    protected GroupByKeyTask<K, V> subTask(
        GroupByKeyTask<K, V> parent,
        GroupByKeyTask<K, V> next,
        List<Spliterator<WindowedValue<KV<K, V>>>> splits,
        int lo,
        int hi) {
      return new GroupByKeyTask<>(parent, next, splits, lo, hi);
    }

    @Override
    protected Map<K, Iterable<V>> add(
        @Nullable Map<K, Iterable<V>> acc, WindowedValue<KV<K, V>> wv) {
      if (acc == null) {
        acc = new HashMap<>();
      }
      KV<K, V> kv = wv.getValue();
      acc.compute(kv.getKey(), (k, v) -> append((List<V>) v, kv.getValue()));
      return acc;
    }

    private List<V> append(List<V> list, V value) {
      if (list == null) {
        list = new ArrayList<>();
      }
      list.add(value);
      return list;
    }

    @Override
    protected Map<K, Iterable<V>> merge(Map<K, Iterable<V>> left, Map<K, Iterable<V>> right) {
      for (Map.Entry<K, Iterable<V>> e : right.entrySet()) {
        left.compute(e.getKey(), (k, v) -> v != null ? concat(v, e.getValue()) : e.getValue());
      }
      return left;
    }

    @Override
    protected Collection<WindowedValue<KV<K, Iterable<V>>>> getOutput(
        @Nullable Map<K, Iterable<V>> acc) {
      return acc != null
          ? transform(acc.entrySet(), e -> valueInGlobalWindow(KV.of(e.getKey(), e.getValue())))
          : Collections.EMPTY_LIST;
    }
  }
}
