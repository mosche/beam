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

import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

@Internal
@SuppressWarnings("unused")
public abstract class PipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class);

  protected abstract @Nullable <
          InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform);

  public Future<Void> evaluate(
      Pipeline pipeline, LocalPipelineOptions options, MetricsContainerStepMap metrics) {
    DependencyVisitor dependencies = new DependencyVisitor();
    pipeline.traverseTopologically(dependencies);

    EagerEvaluationVisitor translator =
        new EagerEvaluationVisitor(options, metrics, dependencies.results);
    pipeline.traverseTopologically(translator);
    return translator.completion;
  }

  private static final class TranslationResult<T1, T2> {
    private @Nullable PCollection<T1> mainIn;
    private final Set<PTransform<?, ?>> subscribers = new HashSet<>();
    private @MonotonicNonNull Dataset<T2, ?> dataset = null;
    private @Nullable Translation<T1, T2> translation = null;

    TranslationResult(@Nullable PCollection<T1> mainIn) {
      this.mainIn = mainIn;
    }
  }

  private class EagerEvaluationVisitor extends PTransformVisitor
      implements TransformTranslator.Context<PInput, POutput, PTransform<PInput, POutput>> {
    private final Map<PCollection<?>, TranslationResult<?, ?>> translationResults;
    private final LocalPipelineOptions options;
    private final MetricsContainerStepMap metrics;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final Set<Disposable> leaves = new HashSet<>();
    // initialized to 1 to not complete before visiting all nodes
    private final AtomicInteger pendingLeaves = new AtomicInteger(1);
    private final Consumer<? super Throwable> onError;
    private final Runnable onComplete;

    // Mutable transform currently passed to the transform translator
    private @Nullable AppliedPTransform<?, ?, ?> currentTransform = null;

    EagerEvaluationVisitor(
        LocalPipelineOptions options,
        MetricsContainerStepMap metrics,
        Map<PCollection<?>, TranslationResult<?, ?>> translationResults) {
      this.translationResults = translationResults;
      this.options = options;
      this.metrics = metrics;
      this.onError =
          e -> {
            LOG.error("Received error", e);
            leaves.forEach(Disposable::dispose);
            completion.completeExceptionally(e);
          };
      this.onComplete =
          () -> {
            if (pendingLeaves.decrementAndGet() == 0) {
              completion.complete(null);
              // the default cached parallel scheduler won't be disposed
              options.getScheduler().dispose();
            }
          };
    }

    @Override
    public void leavePipeline(Pipeline pipeline) {
      super.leavePipeline(pipeline);
      if (pendingLeaves.decrementAndGet() == 0) {
        completion.complete(null);
      }
    }

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {
      try {
        LOG.debug("Translating {}", node.getFullName());
        currentTransform = node.toAppliedPTransform(getPipeline());
        translator.translate((TransformTranslator.Context) this);
      } catch (Exception e) {
        LOG.error("Error during pipeline translation", e);
        onError.accept(e);
        throw new RuntimeException(e);
      } finally {
        currentTransform = null;
      }
    }

    @Override
    public AppliedPTransform<PInput, POutput, PTransform<PInput, POutput>> getAppliedTransform() {
      return (AppliedPTransform) checkStateNotNull(currentTransform);
    }

    private <T1, T2> TranslationResult<T1, T2> getResult(PCollection<T2> pCollection) {
      return (TranslationResult<T1, T2>) checkStateNotNull(translationResults.get(pCollection));
    }

    private <T1, T2> Dataset<T2, ?> getOrBuildDataset(TranslationResult<T1, T2> res) {
      if (res.dataset == null) {
        Translation<T1, T2> fn = checkStateNotNull(res.translation);
        PCollection<T1> input = checkStateNotNull(res.mainIn);
        // FIXME discard input if no more usage
        res.dataset = checkStateNotNull(getResult(input).dataset).transform(fn, options);
        res.translation = null;
        res.mainIn = null;
      }
      return res.dataset;
    }

    @Override
    public <T> void provide(PCollection<T> pCollection, Dataset<T, ?> dataset) {
      getResult(pCollection).dataset = dataset;
    }

    @Override
    public <T> Dataset<T, ?> require(PCollection<T> pCollection) {
      return getOrBuildDataset(getResult(pCollection));
    }

    @Override
    public <T1, T2> void translate(PCollection<T2> pCollection, Translation<T1, T2> fn) {
      TranslationResult<T1, T2> current = getResult(pCollection);
      TranslationResult<T1, T1> input = getResult(checkStateNotNull(current.mainIn));

      current.translation = fn;

      if (fn instanceof Translation.CanFuse
          && ((Translation.CanFuse<T1, T2>) fn).fuse(input.translation)) {
        PCollection<?> obsoleteMainIn = checkStateNotNull(current.mainIn);
        current.mainIn = checkStateNotNull(input.mainIn);
        // Fused the input into the current translation result, input is obsolete by now.
        // TODO: Watch out if supporting multiple outs, these depend on input as well.
        translationResults.remove(obsoleteMainIn);
      } else {
        getOrBuildDataset(input); // make sure the publisher for prev is build
      }

      int subscribers = current.subscribers.size();
      if (subscribers == 0) {
        evaluateLeaf(getOrBuildDataset(current));
      } else if (subscribers > 1) {
        getOrBuildDataset(current).cache();
      }
    }

    private void evaluateLeaf(Dataset<?, ?> ds) {
      if (completion.isDone()) {
        return; // already completed exceptionally or cancelled
      }
      // Increment pending leaves and start evaluation.
      pendingLeaves.incrementAndGet();
      leaves.add(ds.evaluate(onError, onComplete));
    }

    @Override
    public MetricsContainer getMetricsContainer() {
      return metrics.getContainer(getAppliedTransform().getFullName());
    }

    @Override
    public boolean isLeaf(PCollection<?> pCollection) {
      return getResult(pCollection).subscribers.isEmpty();
    }

    @Override
    public LocalPipelineOptions getOptions() {
      return options;
    }
  }

  /**
   * {@link PTransformVisitor} that analyses dependencies of supported {@link PTransform
   * PTransforms} to help identify cache candidates.
   *
   * <p>The visitor may throw if a {@link PTransform} is observed that uses unsupported features.
   */
  private class DependencyVisitor extends PTransformVisitor {
    private final Map<PCollection<?>, TranslationResult<?, ?>> results = new HashMap<>();

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {
      Set<TupleTag<?>> otherIns = transform.getAdditionalInputs().keySet();
      AtomicReference<@Nullable PCollection<InT>> mainIn = new AtomicReference<>();
      node.getInputs()
          .forEach(
              (tag, pIn) -> {
                if (!otherIns.contains(tag)) {
                  mainIn.set((PCollection) pIn);
                }
                checkStateNotNull(results.get(pIn)).subscribers.add(transform);
              });

      node.getOutputs()
          .values()
          .forEach(pOut -> results.put(pOut, new TranslationResult<>(mainIn.get())));
    }
  }

  /**
   * An abstract {@link PipelineVisitor} that visits all translatable {@link PTransform} pipeline
   * nodes of a pipeline with the respective {@link TransformTranslator}.
   *
   * <p>The visitor may throw if a {@link PTransform} is observed that uses unsupported features.
   */
  private abstract class PTransformVisitor extends PipelineVisitor.Defaults {

    /** Visit the {@link PTransform} with its respective {@link TransformTranslator}. */
    abstract <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator);

    @Override
    public final CompositeBehavior enterCompositeTransform(Node node) {
      PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getSupportedTranslator(transform);
      if (transform != null && translator != null) {
        visit(node, transform, translator);
        return DO_NOT_ENTER_TRANSFORM;
      } else {
        return ENTER_TRANSFORM;
      }
    }

    @Override
    public final void visitPrimitiveTransform(Node node) {
      PTransform<PInput, POutput> transform = (PTransform<PInput, POutput>) node.getTransform();
      if (transform == null || transform.getClass().equals(View.CreatePCollectionView.class)) {
        return; // ignore, nothing to be translated here, views are handled on the consumer side
      }
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getSupportedTranslator(transform);
      if (translator == null) {
        String urn = PTransformTranslation.urnForTransform(transform);
        throw new UnsupportedOperationException("Transform " + urn + " is not supported.");
      }
      visit(node, transform, translator);
    }

    /** {@link TransformTranslator} for {@link PTransform} if translation is known and supported. */
    private @Nullable TransformTranslator<PInput, POutput, PTransform<PInput, POutput>>
        getSupportedTranslator(@Nullable PTransform<PInput, POutput> transform) {
      if (transform == null) {
        return null;
      }
      TransformTranslator<PInput, POutput, PTransform<PInput, POutput>> translator =
          getTransformTranslator(transform);
      return translator != null && translator.canTranslate(transform) ? translator : null;
    }
  }
}
