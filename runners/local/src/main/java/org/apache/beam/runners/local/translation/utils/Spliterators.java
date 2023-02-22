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
package org.apache.beam.runners.local.translation.utils;

import static avro.shaded.com.google.common.base.Objects.firstNonNull;
import static java.util.Spliterators.emptySpliterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;

public class Spliterators {
  private Spliterators() {}

  /** Adapter from {@link Spliterator} to {@link Iterator}. */
  public static <T> Iterator<T> asIterator(Spliterator<T> split) {
    return new SpliteratorAsIterator<>(split);
  }

  public static <T> Spliterator<T> concat(Spliterator<T> a, Spliterator<T> b) {
    if (a.getExactSizeIfKnown() == 0) {
      return b;
    } else if (b.getExactSizeIfKnown() == 0) {
      return b;
    } else {
      return RandomUtils.nextBoolean()
          ? new ConcatSpliterator<>(a, b)
          : new ConcatSpliterator<>(b, a);
    }
  }

  public static <T> Spliterator<T> concat(Iterable<Spliterator<T>> splits) {
    Spliterator<T> res = null;
    for (Spliterator<T> split : splits) {
      res = (res == null) ? split : Spliterators.concat(res, split);
    }
    return res != null ? res : emptySpliterator();
  }

  public static <T1, T2> Spliterator<T2> map(Spliterator<T1> spliterator, Function<T1, T2> fn) {
    return new MapSpliterator<>(spliterator, fn);
  }

  private static class SpliteratorAsIterator<T> implements Iterator<T> {
    private final Spliterator<T> split;
    private @Nullable T next = null;

    SpliteratorAsIterator(Spliterator<T> split) {
      this.split = split;
    }

    @Override
    public boolean hasNext() {
      return next != null || split.tryAdvance(v -> next = v);
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      T current = next;
      next = null;
      return current;
    }
  }

  private static class ConcatSpliterator<T> implements Spliterator<T> {
    private @Nullable Spliterator<T> a;
    private @Nullable Spliterator<T> b;

    // FIXME both must be sized
    ConcatSpliterator(Spliterator<T> a, Spliterator<T> b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
      if (a != null) {
        if (a.tryAdvance(action)) {
          return true;
        }
        a = null;
      }
      if (b != null) {
        if (b.tryAdvance(action)) {
          return true;
        }
        b = null;
      }
      return false;
    }

    @Override
    public Spliterator<T> trySplit() {
      if (a == null && b == null) {
        return null;
      } else if (b == null) {
        return a.trySplit();
      } else if (a == null) {
        return b.trySplit();
      }

      long aSize = a.estimateSize();
      long bSize = b.estimateSize();
      if (aSize + bSize < 100) {
        return null; // avoid small splits
      }

      long aRatio = aSize / (aSize + bSize);
      if (0.4 <= aRatio && aRatio <= 0.6) {
        Spliterator<T> split = b;
        b = null;
        return split;
      } else if (aRatio > 0.6) {
        Spliterator<T> split = a.trySplit();
        split = split != null ? new ConcatSpliterator<>(split, b) : b;
        b = null;
        return split;
      } else {
        return b.trySplit();
      }
    }

    @Override
    public long estimateSize() {
      return (a != null ? a.estimateSize() : 0) + (b != null ? b.estimateSize() : 0);
    }

    @Override
    public int characteristics() {
      return firstNonNull(a, b).characteristics();
    }
  }

  /** Spliterator lazily applying a function t1 -> t2 to each record. */
  private static class MapSpliterator<T1, T2> implements Spliterator<T2> {
    final Spliterator<T1> spliterator;
    final Function<T1, T2> mapFn;

    MapSpliterator(Spliterator<T1> spliterator, Function<T1, T2> fn) {
      this.spliterator = spliterator;
      this.mapFn = fn;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T2> action) {
      return spliterator.tryAdvance(t1 -> action.accept(mapFn.apply(t1)));
    }

    @Override
    public Spliterator<T2> trySplit() {
      Spliterator<T1> split = spliterator.trySplit();
      return split != null ? new MapSpliterator<>(split, mapFn) : null;
    }

    @Override
    public long estimateSize() {
      return spliterator.estimateSize();
    }

    @Override
    public int characteristics() {
      return spliterator.characteristics();
    }
  }
}
