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
import static java.util.Collections.EMPTY_LIST;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.Spliterator;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

public class ConcatList<T> extends AbstractList<T> implements RandomAccess {
  private final List<T> a, b;

  public static <T> List<T> of(@Nullable List<T> a, @Nullable List<T> b) {
    if (a == null || a.isEmpty()) {
      return firstNonNull(b, EMPTY_LIST);
    } else if (b == null || b.isEmpty()) {
      return firstNonNull(a, EMPTY_LIST);
    }
    return new ConcatList<>(a, b);
  }

  private ConcatList(List<T> a, List<T> b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public Iterator<T> iterator() {
    return Iterators.concat(a.iterator(), b.iterator());
  }

  @Override
  public Spliterator<T> spliterator() {
    return Spliterators.concat(a.spliterator(), b.spliterator());
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    int aSize = a.size();
    if (toIndex <= aSize) {
      return a.subList(fromIndex, toIndex);
    } else if (fromIndex >= aSize) {
      return b.subList(fromIndex - aSize, toIndex - aSize);
    } else {
      return new ConcatList<>(a.subList(fromIndex, aSize), b.subList(0, toIndex - aSize));
    }
  }

  @Override
  public T get(int index) {
    return index < a.size() ? a.get(index) : b.get(index - a.size());
  }

  @Override
  public int size() {
    return a.size() + b.size();
  }

  @Override
  public boolean add(T t) {
    return b.add(t);
  }
}
