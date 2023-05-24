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
package org.apache.beam.runners.reactor.translation;

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface Dataset<T, FluxT> {
  static Dataset<byte[], ?> impulse() {
    return SimpleDataset.IMPULSE;
  }

  <T2> Dataset<T2, ?> transform(Translation<T, T2> t, int subscribers, ReactorOptions opts);

  <T2> Map<TupleTag<?>, Dataset<T2, ?>> transformTagged(
      Translation<T, T2> t, Map<TupleTag<?>, Integer> subscribers, ReactorOptions opts);

  Disposable evaluate(Consumer<? super Throwable> onError, Runnable onComplete);

  Dataset<T, FluxT> union(Dataset<T, FluxT> other);

  Mono<List<WindowedValue<T>>> collect();

  class SimpleDataset<T> implements Dataset<T, Flux<T>> {
    private static final Dataset<byte[], ?> IMPULSE =
        new SimpleDataset<>(Flux.just(valueInGlobalWindow(EMPTY_BYTE_ARRAY)));

    private final Flux<WindowedValue<T>> flux;

    private SimpleDataset(Flux<WindowedValue<T>> flux) {
      this.flux = flux;
    }

    @Override
    public <T2> Dataset<T2, ?> transform(
        Translation<T, T2> t, int subscribers, ReactorOptions opts) {
      return new SimpleDataset<>(t.simple(flux, subscribers, opts));
    }

    @Override
    public <T2> Map<TupleTag<?>, Dataset<T2, ?>> transformTagged(
        Translation<T, T2> t, Map<TupleTag<?>, Integer> subscribers, ReactorOptions opts) {
      return Maps.transformValues(t.simpleTagged(flux, subscribers, opts), SimpleDataset::new);
    }

    @Override
    public Disposable evaluate(Consumer<? super Throwable> onError, Runnable onComplete) {
      return flux.subscribe(null, onError, onComplete);
    }

    @Override
    public Dataset<T, Flux<T>> union(Dataset<T, Flux<T>> other) {
      return new SimpleDataset<>(flux.concatWith(((SimpleDataset<T>) other).flux));
    }

    @Override
    public Mono<List<WindowedValue<T>>> collect() {
      return flux.collectList();
    }
  }
}
