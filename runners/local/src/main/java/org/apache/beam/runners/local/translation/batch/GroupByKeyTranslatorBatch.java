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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Collections2.transform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTask;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.runners.local.translation.utils.ConcatList;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Converter;

class GroupByKeyTranslatorBatch<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  @Override
  public void translate(GroupByKey<K, V> transform, Context cxt) {
    PCollection<KV<K, V>> input = cxt.getInput();
    Dataset<KV<K, V>> dataset = cxt.requireDataset(input);

    KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Converter<K, ?> keyFn =
        (coder.getKeyCoder() instanceof RowCoder)
            ? RowKeyConverter.of((RowCoder) coder.getKeyCoder())
            : Converter.identity();

    cxt.provideDataset(
        cxt.getOutput(), dataset.evaluate(new GroupByKeyTask<>(cxt.fullName(), keyFn)));
  }

  private static class GroupByKeyTask<K, KIntT, V>
      extends TransformTask<
          KV<K, V>, Map<KIntT, List<V>>, Collection<WindowedValue<KV<K, Iterable<V>>>>> {
    private final Converter<K, KIntT> keyFn;

    private GroupByKeyTask(String name, Converter<K, KIntT> keyFn) {
      super(name);
      this.keyFn = keyFn;
    }

    @Override
    protected Map<KIntT, List<V>> add(
        @Nullable Map<KIntT, List<V>> acc, WindowedValue<KV<K, V>> wv, int idx) {
      if (acc == null) {
        acc = new HashMap<>();
      }
      KV<K, V> kv = wv.getValue();
      KIntT intKey = keyFn.convert(kv.getKey());
      List<V> perKey = acc.get(intKey);
      if (perKey == null) {
        perKey = new ArrayList<>();
        acc.put(intKey, perKey);
      }
      perKey.add(kv.getValue());
      return acc;
    }

    @Override
    protected Map<KIntT, List<V>> merge(Map<KIntT, List<V>> left, Map<KIntT, List<V>> right) {
      if (right.size() > left.size()) {
        return merge(right, left);
      }
      for (Map.Entry<KIntT, List<V>> e : right.entrySet()) {
        left.compute(e.getKey(), (k, v) -> ConcatList.of(v, e.getValue()));
      }
      return left;
    }

    @Override
    protected Collection<WindowedValue<KV<K, Iterable<V>>>> getOutput(
        @Nullable Map<KIntT, List<V>> acc) {
      if (acc == null) {
        return Collections.EMPTY_LIST;
      }
      Converter<KIntT, K> reverseFn = keyFn.reverse();
      return transform(
          acc.entrySet(),
          e -> valueInGlobalWindow(KV.of(reverseFn.convert(e.getKey()), e.getValue())));
    }
  }

  private static class RowKeyConverter extends Converter<Row, List<Object>> {
    final Schema schema;

    static <K> Converter<K, ?> of(RowCoder coder) {
      // checkFields(coder.getSchema());
      return (Converter<K, ?>) new RowKeyConverter(coder.getSchema());
    }

    //    private static void checkFields(Schema schema) {
    //      for (Field f : schema.getFields()) {
    //        checkState(BYTES.equals(f.getType()), "Type BYTES not supported [%s]", f.getName());
    //        Schema fSchema = f.getType().getRowSchema();
    //        if (fSchema != null) {
    //          checkFields(fSchema);
    //        }
    //      }
    //    }

    private RowKeyConverter(Schema schema) {
      this.schema = schema;
    }

    @Override
    protected List<Object> doForward(Row row) {
      return row.getValues();
    }

    @Override
    protected Row doBackward(List<Object> values) {
      return Row.withSchema(schema).attachValues(values);
    }
  }
}
