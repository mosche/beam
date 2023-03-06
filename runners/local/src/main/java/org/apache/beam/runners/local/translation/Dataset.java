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
package org.apache.beam.runners.local.translation;

import static java.util.Collections.EMPTY_LIST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.local.translation.utils.ConcatList;
import org.apache.beam.runners.local.translation.utils.Spliterable;
import org.apache.beam.runners.local.translation.utils.Spliterators;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

@SuppressWarnings("unused")
public abstract class Dataset<T> implements Spliterable<WindowedValue<T>> {

  public static <T, SplitT extends Iterable<WindowedValue<T>>> Dataset<T> ofSplits(
      String name, List<SplitT> splits) {
    return new PreSplit<>(name, splits);
  }

  public static <T> Dataset<T> ofRecords(
      String name, List<WindowedValue<T>> records, int splitLevels) {
    return new Records<>(name, records, splitLevels);
  }

  public static <T> Dataset<T> union(String name, List<Dataset<T>> datasets) {
    return new Union<>(name, datasets);
  }

  protected final int splitLevels;
  protected final String name;

  private Dataset(String name, int splitLevels) {
    this.name = name;
    this.splitLevels = splitLevels;
  }

  public <T2> Dataset<T2> evaluate(TransformTask<T, ?, Collection<WindowedValue<T2>>> tf) {
    return new Records<>(name, tf.evaluate(this), splitLevels);
  }

  public interface TransformFn<T1, T2>
      extends Function<Spliterator<WindowedValue<T1>>, Spliterator<WindowedValue<T2>>> {}

  public interface MapFn<T1, T2> extends Function<WindowedValue<T1>, WindowedValue<T2>> {}

  public <T2> Dataset<T2> transform(String name, TransformFn<T, T2> fn) {
    return new Transformed<>(name, this, fn);
  }

  public <T2> Dataset<T2> map(String name, MapFn<T, T2> mapFn) {
    return transform(name, s -> Spliterators.map(s, mapFn));
  }

  public abstract List<Spliterator<WindowedValue<T>>> spliterators();

  public Dataset<T> collect() {
    return evaluate(new PersistTask<T>(name));
  }

  public void evaluate() {
    new EvalTask<T>(name).evaluate(this);
  }

  /** In-memory dataset wrapping a collection of records. */
  private static class Records<T> extends Dataset<T> {
    final Collection<WindowedValue<T>> col;

    Records(String name, Collection<WindowedValue<T>> col, int splits) {
      super(name, splits);
      this.col = col;
    }

    @Override
    public Dataset<T> collect() {
      return this;
    }

    @Override
    public List<Spliterator<WindowedValue<T>>> spliterators() {
      return createSplits(spliterator(), splitLevels);
    }

    @Override
    public Spliterator<WindowedValue<T>> spliterator() {
      return col.spliterator();
    }

    @Override
    public Iterator<WindowedValue<T>> iterator() {
      return col.iterator();
    }

    @Override
    public String toString() {
      return "Records[" + col.size() + "]";
    }
  }

  /** Dataset pre-split into multiple iterables. */
  private static class PreSplit<T, SplitT extends Iterable<WindowedValue<T>>> extends Dataset<T> {
    final List<SplitT> splits;

    PreSplit(String name, List<SplitT> splits) {
      super(name, splits.size());
      this.splits = splits;
    }

    @Override
    public List<Spliterator<WindowedValue<T>>> spliterators() {
      List<Spliterator<WindowedValue<T>>> spliterators = new ArrayList<>(splits.size());
      for (SplitT it : splits) {
        spliterators.add(it.spliterator());
      }
      return spliterators;
    }

    @Override
    public Spliterator<WindowedValue<T>> spliterator() {
      return Spliterators.concat(spliterators());
    }

    @Override
    public String toString() {
      return "PreSplit[" + splits.size() + "]";
    }
  }

  /** Dataset as union of several datasets, collected as in-memory {@link PreSplit}. */
  private static class Union<T> extends Dataset<T> {
    final List<Dataset<T>> datasets;

    public Union(String name, List<Dataset<T>> datasets) {
      super(name, datasets.size() > 0 ? datasets.get(0).splitLevels : 0);
      this.datasets = datasets;
    }

    @Override
    public List<Spliterator<WindowedValue<T>>> spliterators() {
      return createSplits(spliterator(), splitLevels);
    }

    @SuppressWarnings("nullable")
    @Override
    public Spliterator<WindowedValue<T>> spliterator() {
      return Spliterators.concat(Iterables.transform(datasets, ds -> ds.spliterator()));
    }

    @Override
    public String toString() {
      return "Union" + Iterables.toString(datasets);
    }
  }

  /** Lazily transformed dataset, collected as in-memory {@link PreSplit}. */
  private static class Transformed<T1, T2> extends Dataset<T2> {
    final Dataset<T1> dataset;
    final TransformFn<T1, T2> fn;

    Transformed(String name, Dataset<T1> dataset, TransformFn<T1, T2> fn) {
      super(name, dataset.splitLevels);
      this.dataset = dataset;
      this.fn = fn;
    }

    @Override
    public <T3> Dataset<T3> transform(String name, TransformFn<T2, T3> fn) {
      return new Transformed<>(name, dataset, t1 -> fn.apply(this.fn.apply(t1)));
    }

    @Override
    public List<Spliterator<WindowedValue<T2>>> spliterators() {
      List<Spliterator<WindowedValue<T1>>> splits = dataset.spliterators();
      List<Spliterator<WindowedValue<T2>>> transformed = new ArrayList<>(splits.size());
      for (Spliterator<WindowedValue<T1>> split : splits) {
        transformed.add(fn.apply(split));
      }
      return transformed;
    }

    @Override
    public Spliterator<WindowedValue<T2>> spliterator() {
      return fn.apply(dataset.spliterator());
    }

    @Override
    public String toString() {
      return "Transformed[" + dataset + "]";
    }
  }

  private static <X> List<Spliterator<X>> createSplits(Spliterator<X> split, int splitLevels) {
    List<Spliterator<X>> splits = new ArrayList<>(2 ^ splitLevels);
    splits.add(split);
    createSplits(split, splitLevels, splits);
    return splits;
  }

  private static <X> void createSplits(
      Spliterator<X> sit, int levels, List<Spliterator<X>> splits) {
    if (levels == 0 || sit.estimateSize() < 100) {
      return;
    }
    Spliterator<X> next = sit.trySplit();
    if (next != null) {
      createSplits(next, levels - 1, splits);
      splits.add(next);
      createSplits(next, levels - 1, splits);
    }
  }

  /** Force evaluation for leaves. */
  private static class EvalTask<T> extends TransformTask<T, Void, Void> {
    EvalTask(String name) {
      super(name);
    }

    @Override
    protected Void add(@Nullable Void acc, WindowedValue<T> in, int idx) {
      return null;
    }

    @Override
    protected Void merge(Void left, Void right) {
      return null;
    }

    @Override
    protected Void getOutput(@Nullable Void acc) {
      return null;
    }
  }

  /** Fork-join task to persist all {@link Dataset#spliterators()} in-memory as {@link Records}. */
  private static class PersistTask<T>
      extends TransformTask<T, List<WindowedValue<T>>, Collection<WindowedValue<T>>> {

    PersistTask(String name) {
      super(name);
    }

    @Override
    protected List<WindowedValue<T>> add(
        @Nullable List<WindowedValue<T>> acc, WindowedValue<T> wv, int idx) {
      if (acc == null) {
        acc = new ArrayList<>();
      }
      acc.add(wv);
      return acc;
    }

    @Override
    protected List<WindowedValue<T>> merge(List<WindowedValue<T>> a, List<WindowedValue<T>> b) {
      return ConcatList.of(a, b);
    }

    @Override
    protected Collection<WindowedValue<T>> getOutput(@Nullable List<WindowedValue<T>> acc) {
      return acc != null ? acc : EMPTY_LIST;
    }
  }
}
