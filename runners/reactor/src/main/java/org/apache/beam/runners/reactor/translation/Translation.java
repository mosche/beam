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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import reactor.core.publisher.Flux;

public interface Translation<T1, T2> {
  static <T> Identity<T> identity() {
    return (Identity<T>) Identity.INSTANCE;
  }

  Flux<WindowedValue<T2>> simple(
      Flux<WindowedValue<T1>> flux, int subscribers, ReactorOptions opts);

  default Map<TupleTag<?>, Flux<WindowedValue<T2>>> simpleTagged(
      Flux<WindowedValue<T1>> fluxIn, Map<TupleTag<?>, Integer> subscribers, ReactorOptions opts) {
    throw new UnsupportedOperationException();
  }

  interface CanFuse<T1, T2> extends Translation<T1, T2> {
    <T0> boolean fuse(@Nullable Translation<T0, T1> prev);
  }

  abstract class BasicTranslation<T1, T2> implements Translation<T1, T2> {
    @Override
    public Flux<WindowedValue<T2>> simple(
        Flux<WindowedValue<T1>> flux, int subscribers, ReactorOptions opts) {
      Flux<WindowedValue<T2>> result = simple(flux, opts);
      return subscribers > 1 ? result.publish().refCount(subscribers) : result;
    }

    public abstract Flux<WindowedValue<T2>> simple(
        Flux<WindowedValue<T1>> flux, ReactorOptions opts);
  }

  class Identity<T> implements Translation<T, T> {
    private static final Identity<?> INSTANCE = new Identity<>();

    @Override
    public Flux<WindowedValue<T>> simple(
        Flux<WindowedValue<T>> flux, int subscribers, ReactorOptions opts) {
      return flux;
    }
  }
}
