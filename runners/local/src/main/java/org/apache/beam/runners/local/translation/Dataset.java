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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;

public abstract class Dataset<T> {
  public static <T, SplitT extends Iterable<WindowedValue<T>>> Dataset<T> ofSplits(
      List<SplitT> splits) {
    return new PreSplitDataset<>(splits, false);
  }

  public static <T> Dataset<T> of(List<WindowedValue<T>> records, int splitLevels) {
    return new CollectionDataset<>(records, splitLevels);
  }

  public static <T> Dataset<T> union(List<Dataset<T>> datasets) {
    return new UnionDataset<>(datasets);
  }

  protected final int splitLevels;

  private Dataset(int splitLevels) {
    this.splitLevels = splitLevels;
  }

  public <T2> Dataset<T2> transform(
      TransformTask.Factory<WindowedValue<T>, Collection<WindowedValue<T2>>> factory) {
    return new CollectionDataset<>(factory.create(spliterators()).invoke(), splitLevels);
  }

  public interface TransformFn<T1, T2>
      extends Function<Spliterator<WindowedValue<T1>>, Spliterator<WindowedValue<T2>>> {}

  public <T2> Dataset<T2> lazyTransform(TransformFn<T, T2> fn) {
    return new TransformedDataset<>(this, fn);
  }

  public interface MapFn<T1, T2> extends Function<WindowedValue<T1>, WindowedValue<T2>>{}

  public <T2> Dataset<T2> map(MapFn<T, T2> mapFn) {
    return lazyTransform(s -> new MapSpliterator<>(s, mapFn));
  }

  public abstract List<Spliterator<WindowedValue<T>>> spliterators();

  public abstract Iterable<WindowedValue<T>> iterable();

  public abstract Dataset<T> collect();

  /** In-memory dataset wrapping a collection of records. */
  private static class CollectionDataset<T> extends Dataset<T> {
    final Collection<WindowedValue<T>> col;

    CollectionDataset(Collection<WindowedValue<T>> col, int splits) {
      super(splits);
      this.col = col;
    }

    @Override
    public Dataset<T> collect() {
      return this;
    }

    @Override
    public List<Spliterator<WindowedValue<T>>> spliterators() {
      return createSplits(col.spliterator(), splitLevels);
    }

    @Override
    public Iterable<WindowedValue<T>> iterable() {
      return col;
    }

    private static <T> List<Spliterator<T>> createSplits(Spliterator<T> spliterator, int levels) {
      ArrayList<Spliterator<T>> akk = new ArrayList<>(2 ^ levels);
      akk.add(spliterator);
      createSplits(spliterator, levels, akk);
      return akk;
    }

    private static <T> void createSplits(
        Spliterator<T> spliterator, int levels, List<Spliterator<T>> akk) {
      if (levels == 0) {
        return;
      }
      Spliterator<T> split = spliterator.trySplit();
      if (split != null) {
        createSplits(spliterator, levels - 1, akk);
        akk.add(split);
        createSplits(split, levels - 1, akk);
      }
    }
  }

  /** Dataset pre-split into multiple iterables (possible being in-memory). */
  private static class PreSplitDataset<T, SplitT extends Iterable<WindowedValue<T>>>
      extends Dataset<T> {
    final List<SplitT> splits;
    final boolean inMemory;

    PreSplitDataset(List<SplitT> splits, boolean inMemory) {
      super(splits.size());
      this.splits = splits;
      this.inMemory = inMemory;
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
    public Iterable<WindowedValue<T>> iterable() {
      return Iterables.concat(splits);
    }

    @Override
    public Dataset<T> collect() {
      return inMemory ? this : new PersistTask<>(spliterators()).invoke();
    }
  }

  /** Dataset as union of several datasets, collected as in-memory {@link PreSplitDataset}. */
  private static class UnionDataset<T> extends Dataset<T> {
    final List<Dataset<T>> datasets;

    public UnionDataset(List<Dataset<T>> datasets) {
      super(datasets.size() > 0 ? datasets.get(0).splitLevels : 0);
      this.datasets = datasets;
    }

    @Override
    public List<Spliterator<WindowedValue<T>>> spliterators() {
      List<Spliterator<WindowedValue<T>>> list = new ArrayList<>();
      for (Dataset<T> ds : datasets) {
        list.addAll(ds.spliterators());
      }
      return list;
    }

    @Override
    public Iterable<WindowedValue<T>> iterable() {
      List<Iterable<WindowedValue<T>>> iterables = new ArrayList<>(datasets.size());
      for (Dataset<T> ds : datasets) {
        iterables.add(ds.iterable());
      }
      return Iterables.concat(iterables);
    }

    @Override
    public Dataset<T> collect() {
      return new PersistTask<>(spliterators()).invoke();
    }
  }

  /** Lazily transformed dataset, collected as in-memory {@link PreSplitDataset}. */
  private static class TransformedDataset<T1, T2> extends Dataset<T2> {
    final Dataset<T1> dataset;
    final TransformFn<T1, T2> fn;

    TransformedDataset(Dataset<T1> dataset, TransformFn<T1, T2> fn) {
      super(dataset.splitLevels);
      this.dataset = dataset;
      this.fn = fn;
    }

    @Override
    public List<Spliterator<WindowedValue<T2>>> spliterators() {
      List<Spliterator<WindowedValue<T1>>> splits = dataset.spliterators();
      List<Spliterator<WindowedValue<T2>>> spliterators = new ArrayList<>(splits.size());
      for (Spliterator<WindowedValue<T1>> split : splits) {
        spliterators.add(fn.apply(split));
      }
      return spliterators;
    }

    @Override
    public Iterable<WindowedValue<T2>> iterable() {
      return () -> {
        List<Spliterator<WindowedValue<T1>>> splits = dataset.spliterators();
        List<Iterator<WindowedValue<T2>>> iterators = new ArrayList<>(splits.size());
        for (Spliterator<WindowedValue<T1>> split : splits) {
          iterators.add(new SpliteratorAsIterator<>(fn.apply(split)));
        }
        return Iterators.concat(iterators.iterator());
      };
    }

    @Override
    public Dataset<T2> collect() {
      return new PersistTask<>(spliterators()).invoke();
    }
  }

  /**
   * Spliterable offers a first class {@link Spliterator}. The corresponding {@link Iterator} is
   * derived from the {@link Spliterator} rather then the other way around.
   */
  public interface Spliterable<T> extends Iterable<T> {
    @Override
    default Iterator<T> iterator() {
      return new SpliteratorAsIterator<>(spliterator());
    }

    @Override
    Spliterator<T> spliterator();
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

  /**
   * Fork-join task to persist all {@link Dataset#spliterators()} in-memory as {@link
   * PreSplitDataset}.
   */
  private static class PersistTask<T, W extends WindowedValue<T>>
      extends TransformTask<W, List<List<W>>, Dataset<T>, PersistTask<T, W>> {
    private final List<W> records = new ArrayList<>();

    PersistTask(List<Spliterator<W>> splits) {
      super(null, null, splits, 0, splits.size());
    }

    private PersistTask(
        @Nullable PersistTask<T, W> parent,
        @Nullable PersistTask<T, W> next,
        List<Spliterator<W>> splits,
        int lo,
        int hi) {
      super(parent, next, splits, lo, hi);
    }

    @Override
    protected PersistTask<T, W> subTask(
        PersistTask<T, W> parent,
        PersistTask<T, W> next,
        List<Spliterator<W>> splits,
        int lo,
        int hi) {
      return new PersistTask<>(parent, next, splits, lo, hi);
    }

    @Override
    protected List<List<W>> add(@Nullable List<List<W>> acc, W wv) {
      if (acc == null) {
        acc = new ArrayList<>();
        acc.add(records);
      }
      records.add(wv);
      return acc;
    }

    @Override
    protected List<List<W>> merge(List<List<W>> left, List<List<W>> right) {
      left.addAll(right);
      return left;
    }

    @Override
    protected Dataset<T> getOutput(@Nullable List<List<W>> acc) {
      return new PreSplitDataset<>(acc != null ? (List) acc : Collections.EMPTY_LIST, true);
    }
  }

  /** Adapter from {@link Spliterator} to {@link Iterator}. */
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
}
