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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.concat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.Translation;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Converter;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class GroupByKeyTranslatorBatch<K, V>
    extends TransformTranslator<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  @Override
  public void translate(GroupByKey<K, V> transform, Context cxt) {
    PCollection<KV<K, V>> input = cxt.getInput();
    KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Converter<K, ?> keyFn =
        (coder.getKeyCoder() instanceof RowCoder)
            ? RowKeyConverter.of((RowCoder) coder.getKeyCoder())
            : Converter.identity();
    cxt.translate(cxt.getOutput(), new TranslateGroupByKey<>(keyFn));
  }

  private static class TranslateGroupByKey<K, KIntT, V>
      implements Translation<KV<K, V>, KV<K, Iterable<V>>> {
    final Function<WindowedValue<KV<K, V>>, KIntT> keyMapper;
    final Function<WindowedValue<KV<K, V>>, V> valueMapper;
    final Converter<K, KIntT> keyFn;

    @SuppressWarnings("nullness")
    TranslateGroupByKey(Converter<K, KIntT> keyFn) {
      this.keyFn = keyFn;
      keyMapper = wv -> keyFn.convert(wv.getValue().getKey());
      valueMapper = wv -> wv.getValue().getValue();
    }

    private Map<KIntT, Iterable<V>> merge(
        Map<KIntT, Iterable<V>> map1, Map<KIntT, Iterable<V>> map2) {
      if (map2.size() > map1.size()) {
        return merge(map2, map1);
      }
      map2.forEach(
          (key, col2) -> map1.compute(key, (k, col1) -> col1 == null ? col2 : concat(col1, col2)));
      return map1;
    }

    @Override
    public Flux<WindowedValue<KV<K, Iterable<V>>>> simple(
        Flux<WindowedValue<KV<K, V>>> flux, LocalPipelineOptions opts) {
      return flux.collectMultimap(keyMapper, valueMapper)
          .flatMapIterable(Map::entrySet)
          .map(e -> valueInGlobalWindow(KV.of(keyFn.reverse().convert(e.getKey()), e.getValue())));
    }

    @Override
    public Flux<? extends Flux<WindowedValue<KV<K, Iterable<V>>>>> parallel(
        Flux<? extends Flux<WindowedValue<KV<K, V>>>> flux, LocalPipelineOptions opts) {
      Flux<Map<KIntT, Iterable<V>>> maps =
          flux.subscribeOn(opts.getScheduler())
              .flatMap(
                  group -> (Mono) group.collectMultimap(keyMapper, valueMapper),
                  opts.getParallelism());
      return maps.reduce(this::merge)
          .flatMapIterable(Map::entrySet)
          .map(e -> valueInGlobalWindow(KV.of(keyFn.reverse().convert(e.getKey()), e.getValue())))
          .parallel(opts.getParallelism())
          .runOn(opts.getScheduler())
          .groups();
    }
  }

  private static class RowKeyConverter extends Converter<Row, List<@Nullable Object>> {
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
    protected List<@Nullable Object> doForward(Row row) {
      return row.getValues();
    }

    @Override
    @SuppressWarnings("argument")
    protected Row doBackward(List<@Nullable Object> values) {
      return Row.withSchema(schema).attachValues(values);
    }
  }
}
