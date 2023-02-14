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

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.local.LocalPipelineOptions;
import org.apache.beam.runners.local.translation.Dataset;
import org.apache.beam.runners.local.translation.TransformTranslator;
import org.apache.beam.runners.local.translation.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
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
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.beam.runners.core.construction.ParDoTranslation.getSchemaInformation;
import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

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
    MetricsContainer metrics = cxt.getMetricsContainer();
    DoFn<InT, OutT> fn = transform.getFn();
    DoFnSchemaInformation schema = getSchemaInformation(cxt.getCurrentTransform());
    List<TupleTag<?>> additionalOuts = transform.getAdditionalOutputTags().getAll();

    SideInputReader sideInputs = createSideInputs(transform.getSideInputs().values(), cxt);
    String name = cxt.getCurrentTransform().getFullName();

    cxt.putDataset(
        cxt.getOutput(mainOut),
        cxt.getDataset(input)
            .lazyTransform(
                s ->
                    new DoFnSpliterator<>(
                        name, s, opts, fn, mainOut, additionalOuts, sideInputs, schema, metrics)));
  }

  private <T> SideInputReader createSideInputs(Collection<PCollectionView<?>> views, Context cxt) {
    if (views.isEmpty()) {
      return EmptyReader.INSTANCE;
    }
    Map<TupleTag<?>, Dataset<?>> datasets = Maps.newHashMapWithExpectedSize(views.size());
    for (PCollectionView<?> view : views) {
      PCollection<T> pCol = checkStateNotNull((PCollection<T>) view.getPCollection());
      datasets.put(view.getTagInternal(), cxt.getDataset(pCol));
    }
    return new GlobalSideInputReader(datasets);
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

  private static class GlobalSideInputReader implements SideInputReader {
    private final Map<TupleTag<?>, Dataset<?>> datasets;

    GlobalSideInputReader(Map<TupleTag<?>, Dataset<?>> datasets) {
      this.datasets = datasets;
    }

    static <ViewT, T> ViewT iterableView(ViewFn<IterableView<T>, ViewT> fn, Dataset<T> ds) {
      Iterable<T> it = Iterables.transform(ds.iterable(), WindowedValue::getValue);
      return fn.apply(() -> it);
    }

    static <K, V, ViewT> ViewT multimapView(
        ViewFn<MultimapView<K, V>, ViewT> fn, Coder<K> coder, Dataset<KV<K, V>> ds) {
      Iterable<KV<K, V>> it = Iterables.transform(ds.iterable(), WindowedValue::getValue);
      return fn.apply(InMemoryMultimapSideInputView.fromIterable(coder, it));
    }

    @Override
    @SuppressWarnings("unchecked") //
    public <ViewT> @Nullable ViewT get(PCollectionView<ViewT> view, BoundedWindow window) {
      TupleTag<?> tag = view.getTagInternal();
      Dataset<?> dataset = checkStateNotNull(datasets.get(tag), "View %s not available.", view);

      ViewFn<?, ViewT> viewFn = view.getViewFn();
      switch (viewFn.getMaterialization().getUrn()) {
        case ITERABLE_MATERIALIZATION_URN:
          return (ViewT) iterableView((ViewFn) viewFn, dataset);
        case MULTIMAP_MATERIALIZATION_URN:
          Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
          return (ViewT) multimapView((ViewFn) viewFn, keyCoder, (Dataset) dataset);
        default:
          throw new IllegalStateException(
              "Unknown materialization urn: " + viewFn.getMaterialization().getUrn());
      }
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return datasets.containsKey(view.getTagInternal());
    }

    @Override
    public boolean isEmpty() {
      return datasets.isEmpty();
    }
  }

  private static class DoFnSpliterator<InT, OutT>
      implements Spliterator<WindowedValue<OutT>>, DoFnRunners.OutputManager {

    private static final byte CREATED = 0;
    private static final byte RUNNING = 1;
    private static final byte FINISHED = 2;

    private static final StepContext NOOP_STEP_CTX = new NoOpStepContext();
    final ArrayDeque<WindowedValue<OutT>> buffer = new ArrayDeque<>();
    final Spliterator<WindowedValue<InT>> input;

    final TupleTag<OutT> mainOut;
    final int characteristics;
    final DoFnRunner<InT, OutT> runner;
    final String name;

    byte state = CREATED;

    DoFnSpliterator(
        String name,
        Spliterator<WindowedValue<InT>> input,
        LocalPipelineOptions opts,
        DoFn<InT, OutT> fn,
        TupleTag<OutT> mainOut,
        List<TupleTag<?>> additionalOuts,
        SideInputReader sideInputs,
        DoFnSchemaInformation doFnSchema,
        MetricsContainer metrics) {
      this.name = name;
      this.input = input;
      this.mainOut = mainOut;
      this.characteristics = input.characteristics() & ~Spliterator.SIZED;
      this.runner =
          createRunner(opts, fn, mainOut, additionalOuts, sideInputs, doFnSchema, metrics);
      DoFnInvokers.tryInvokeSetupFor(fn, opts);
    }

    DoFnRunner<InT, OutT> createRunner(
        LocalPipelineOptions opts,
        DoFn<InT, OutT> fn,
        TupleTag<OutT> mainOut,
        List<TupleTag<?>> additionalOuts,
        SideInputReader sideInputs,
        DoFnSchemaInformation doFnSchema,
        MetricsContainer metrics) {
      DoFnRunner<InT, OutT> runner =
          DoFnRunners.simpleRunner(
              opts,
              fn,
              sideInputs,
              this,
              mainOut,
              additionalOuts,
              NOOP_STEP_CTX,
              null,
              EMPTY_MAP, // no coders used
              WindowingStrategy.globalDefault(),
              doFnSchema,
              ImmutableMap.of());

      if (opts.isMetricsEnabled()) {
        runner = new DoFnRunnerWithMetrics<>(runner, metrics);
      }
      return runner;
    }

    @Override
    public Spliterator<WindowedValue<OutT>> trySplit() {
      return null; // if needed, delegate split to input
    }

    @Override
    public boolean tryAdvance(Consumer<? super WindowedValue<OutT>> action) {
      try {
        if (state == CREATED) {
          state = RUNNING;
          runner.startBundle();
        }
        while (true) {
          if (!buffer.isEmpty()) {
            WindowedValue<OutT> first = buffer.pollFirst();
            action.accept(first);
            return true;
          }
          if (input.tryAdvance(wv -> runner.processElement(wv))) {
            continue;
          } else if (state == RUNNING) {
            state = FINISHED;
            runner.finishBundle();
          } else {
            // DoFnInvokers.invokerFor(runner.getFn()).invokeTeardown();
            return false;
          }
        }
      } catch (RuntimeException e) {
        // DoFnInvokers.invokerFor(runner.getFn()).invokeTeardown();
        throw e;
      }
    }

    @Override
    public long estimateSize() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return characteristics;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      if (!tag.equals(mainOut)) {
        return; // discard other tags
      }
      buffer.addLast((WindowedValue<OutT>) output);
    }
  }

  private static class NoOpStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException("stateInternals is not supported");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("timerInternals is not supported");
    }
  }
}
