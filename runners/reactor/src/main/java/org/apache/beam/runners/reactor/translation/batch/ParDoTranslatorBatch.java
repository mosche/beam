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

import static avro.shaded.com.google.common.collect.Iterables.transform;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonMap;
import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps.transformValues;

import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.reactor.ReactorOptions;
import org.apache.beam.runners.reactor.translation.Dataset;
import org.apache.beam.runners.reactor.translation.TransformTranslator;
import org.apache.beam.runners.reactor.translation.dofn.DoFnRunnerFactory;
import org.apache.beam.runners.reactor.translation.dofn.DoFnTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ParDoTranslatorBatch<InT, OutT>
    extends TransformTranslator<
        PCollection<? extends InT>, PCollectionTuple, ParDo.MultiOutput<InT, OutT>> {

  @Override
  public boolean canTranslate(ParDo.MultiOutput<InT, OutT> transform) {
    DoFn<InT, OutT> doFn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.signatureForDoFn(doFn);

    checkState(
        !signature.processElement().isSplittable(),
        "Not expected to directly translate splittable DoFn, should have been overridden: %s",
        doFn);

    checkState(
        !signature.usesState() && !signature.usesTimers(),
        "States and timers are not supported for the moment.");

    checkState(
        signature.onWindowExpiration() == null, "onWindowExpiration is not supported: %s", doFn);

    checkState(
        !signature.processElement().requiresTimeSortedInput(),
        "@RequiresTimeSortedInput is not supported for the moment");

    return true;
  }

  private WindowFn<?, ?> getInputWindowFn(
      Context<PCollection<? extends InT>, PCollectionTuple, ParDo.MultiOutput<InT, OutT>> cxt) {
    return cxt.getInput().getWindowingStrategy().getWindowFn();
  }

  @Override
  public void translate(
      Context<PCollection<? extends InT>, PCollectionTuple, ParDo.MultiOutput<InT, OutT>> cxt) {
    checkState(
        getInputWindowFn(cxt) instanceof GlobalWindows
            || getInputWindowFn(cxt) instanceof IdentityWindowFn,
        "Only default WindowingStrategy supported");

    // Filter out obsolete PCollections to only cache when absolutely necessary
    Map<TupleTag<?>, PCollection<?>> outputs = skipObsoleteOutputs(cxt);

    ReactorOptions opts = cxt.getOptions();
    MetricsContainer metrics = opts.isMetricsEnabled() ? cxt.getMetricsContainer() : null;
    Mono<SideInputReader> sideInReader =
        sideInputReader(cxt.getTransform().getSideInputs().values(), cxt);
    DoFnRunnerFactory<InT, OutT> factory =
        DoFnRunnerFactory.simple(opts, cxt.getAppliedTransform(), outputs.keySet(), sideInReader);

    DoFnTranslation<InT, OutT> translation = new DoFnTranslation<>(factory, metrics);
    if (outputs.size() == 1) {
      PCollection<OutT> pOut = (PCollection<OutT>) getOnlyElement(outputs.values());
      cxt.translate(pOut, translation);
    } else {
      Dataset<InT, ?> input = (Dataset<InT, ?>) cxt.require(cxt.getInput());
      input
          .transformTagged(translation, transformValues(outputs, cxt::subscribers), opts)
          .forEach(
              (tag, output) -> {
                PCollection<OutT> pOut = checkStateNotNull((PCollection<OutT>) outputs.get(tag));
                cxt.provide(pOut, output);
              });
    }
  }

  /** Filter out obsolete, unused output tags except for {@code mainTag}. */
  private Map<TupleTag<?>, PCollection<?>> skipObsoleteOutputs(
      Context<PCollection<? extends InT>, PCollectionTuple, ParDo.MultiOutput<InT, OutT>> cxt) {
    Map<TupleTag<?>, PCollection<?>> outs = cxt.getOutputs();
    ParDo.MultiOutput<InT, OutT> tf = cxt.getTransform();
    switch (outs.size()) {
      case 1:
        return outs; // always keep main output
      case 2:
        TupleTag<?> otherTag = tf.getAdditionalOutputTags().get(0);
        return cxt.isLeaf(checkStateNotNull(outs.get(otherTag)))
            ? singletonMap(
                tf.getMainOutputTag(), checkStateNotNull(outs.get(tf.getMainOutputTag())))
            : outs;
      default:
        Map<TupleTag<?>, PCollection<?>> filtered = Maps.newHashMapWithExpectedSize(outs.size());
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : outs.entrySet()) {
          if (e.getKey().equals(tf.getMainOutputTag()) || !cxt.isLeaf(e.getValue())) {
            filtered.put(e.getKey(), e.getValue());
          }
        }
        return filtered;
    }
  }

  private <T> Mono<SideInputReader> sideInputReader(
      Collection<@NonNull PCollectionView<?>> views, Context<?, ?, ?> cxt) {
    if (views.isEmpty()) {
      return EmptyReader.INSTANCE;
    }
    return Flux.fromIterable(views)
        .concatMap(view -> sideInput(checkStateNotNull((PCollectionView<T>) view), cxt))
        .collectMap(KV::getKey, KV::getValue)
        .map(GlobalSideInputReader::new);
  }

  private <T> Mono<KV<TupleTag<?>, Iterable<WindowedValue<T>>>> sideInput(
      PCollectionView<T> view, Context<?, ?, ?> cxt) {
    PCollection<T> pCol = checkStateNotNull((PCollection<T>) view.getPCollection());
    return cxt.require(pCol)
        .collect()
        .map(it -> KV.of(view.getTagInternal(), firstNonNull(it, EMPTY_LIST)));
  }

  private static class EmptyReader implements SideInputReader {
    static final Mono<SideInputReader> INSTANCE = Mono.just(new EmptyReader());

    @Override
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      throw new IllegalArgumentException("Cannot get view from empty SideInputReader");
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }

  private static class GlobalSideInputReader<T> implements SideInputReader {
    private final Map<? extends TupleTag<?>, Iterable<WindowedValue<T>>> datasets;

    GlobalSideInputReader(Map<? extends TupleTag<?>, Iterable<WindowedValue<T>>> datasets) {
      this.datasets = datasets;
    }

    static <ViewT, T> ViewT iterableView(ViewFn<IterableView<T>, ViewT> fn, Iterable<T> it) {
      return fn.apply(() -> it);
    }

    static <K, V, ViewT> ViewT multimapView(
        ViewFn<MultimapView<K, V>, ViewT> fn, Coder<K> coder, Iterable<KV<K, V>> it) {
      return fn.apply(InMemoryMultimapSideInputView.fromIterable(coder, it));
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes", "methodref.receiver"})
    public <ViewT> @Nullable ViewT get(PCollectionView<ViewT> view, BoundedWindow window) {
      TupleTag<?> tag = view.getTagInternal();
      Iterable<WindowedValue<T>> windowedIt =
          checkStateNotNull(datasets.get(tag), "View %s not available.", view);
      Iterable<T> it = transform(windowedIt, WindowedValue::getValue);
      ViewFn viewFn = view.getViewFn();
      switch (viewFn.getMaterialization().getUrn()) {
        case ITERABLE_MATERIALIZATION_URN:
          return (ViewT) iterableView(viewFn, it);
        case MULTIMAP_MATERIALIZATION_URN:
          Coder keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
          return (ViewT) multimapView(viewFn, keyCoder, (Iterable) it);
        default:
          throw new IllegalStateException(
              "Unknown materialization urn: " + viewFn.getMaterialization().getUrn());
      }
    }

    @Override
    public <ViewT> boolean contains(PCollectionView<ViewT> view) {
      return datasets.containsKey(view.getTagInternal());
    }

    @Override
    public boolean isEmpty() {
      return datasets.isEmpty();
    }
  }
}
