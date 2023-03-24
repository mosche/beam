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
import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

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

  private WindowFn<?, ?> getInputWindowFn(Context cxt) {
    return cxt.getInput().getWindowingStrategy().getWindowFn();
  }

  @Override
  public void translate(ParDo.MultiOutput<InT, OutT> transform, Context cxt) throws IOException {
    checkState(
        getInputWindowFn(cxt) instanceof GlobalWindows
            || getInputWindowFn(cxt) instanceof IdentityWindowFn,
        "Only default WindowingStrategy supported");

    TupleTag<OutT> mainOut = transform.getMainOutputTag();
    // Only main allowed. Leaf outputs can be ignored, these are not used.
    boolean isMainOnly =
        cxt.getOutputs().entrySet().stream()
            .allMatch(e -> e.getKey().equals(mainOut) || cxt.isLeaf(e.getValue()));
    checkState(isMainOnly, "Additional outputs are not supported");

    PCollection<InT> input = (PCollection<InT>) cxt.getInput();

    LocalPipelineOptions opts = cxt.getOptions();
    Scheduler scheduler = cxt.getScheduler();
    MetricsContainer metrics = cxt.getMetricsContainer();
    Collection<PCollectionView<?>> views = transform.getSideInputs().values();
    DoFnRunnerFactory<InT, OutT> factory =
        DoFnRunnerFactory.simple(cxt.getCurrentTransform(), input, sideInputReader(views, cxt));

    cxt.translate(cxt.getOutput(mainOut), new DoFnTranslation<>(opts, metrics, scheduler, factory));
  }

  private <T> Mono<SideInputReader> sideInputReader(
      Collection<@NonNull PCollectionView<?>> views, Context cxt) {
    int parallelism = cxt.getOptions().getParallelism();
    Scheduler scheduler = cxt.getScheduler();

    if (views.isEmpty()) {
      return Mono.just(EmptyReader.INSTANCE).subscribeOn(scheduler);
    }

    Iterable<Mono<KV<TupleTag<?>, Iterable<WindowedValue<T>>>>> collectedViews =
        transform(views, view -> sideInput(checkStateNotNull(view), parallelism, cxt));

    return Flux.concat(collectedViews)
        .subscribeOn(scheduler)
        .<TupleTag<?>, Iterable<WindowedValue<T>>>collectMap(KV::getKey, KV::getValue)
        .map(viewsAsMap -> (SideInputReader) new GlobalSideInputReader<>(viewsAsMap))
        .cache();
  }

  private <T> Mono<KV<TupleTag<?>, Iterable<WindowedValue<T>>>> sideInput(
      PCollectionView<?> view, int parallelism, Context cxt) {
    PCollection<T> pCol = checkStateNotNull((PCollection<T>) view.getPCollection());
    return cxt.require(pCol)
        .subscribeOn(cxt.getScheduler())
        .flatMap(f -> f, parallelism)
        .collectList()
        .map(it -> KV.of(view.getTagInternal(), firstNonNull(it, EMPTY_LIST)));
  }

  private static class EmptyReader implements SideInputReader {
    static final SideInputReader INSTANCE = new EmptyReader();

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
    private final Map<TupleTag<?>, Iterable<WindowedValue<T>>> datasets;

    GlobalSideInputReader(Map<TupleTag<?>, Iterable<WindowedValue<T>>> datasets) {
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
