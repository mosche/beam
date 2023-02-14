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

import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.local.LocalPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
import static org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior.ENTER_TRANSFORM;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

@Internal
@SuppressWarnings("unused")
public abstract class PipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslator.class);

  protected abstract @Nullable <
          InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>>
      TransformTranslator<InT, OutT, TransformT> getTransformTranslator(TransformT transform);

  public void evaluate(
      Pipeline pipeline, LocalPipelineOptions options, MetricsContainerStepMap metrics) {
    DependencyVisitor dependencies = new DependencyVisitor();
    pipeline.traverseTopologically(dependencies);

    EagerEvaluationVisitor translator =
        new EagerEvaluationVisitor(options, metrics, dependencies.results);
    pipeline.traverseTopologically(translator);
  }

  private static final class TranslationResult<T> {
    private final String name;

    private @MonotonicNonNull Dataset<T> dataset = null;

    // dependent downstream transforms (if empty this is a leaf)
    private final Set<PTransform<?, ?>> dependentTransforms = new HashSet<>();

    private TranslationResult(PCollection<?> pCol) {
      this.name = pCol.getName();
    }

    public String name() {
      return name;
    }

    public @Nullable Dataset<T> dataset() {
      return dataset;
    }

    private boolean isLeaf() {
      return dependentTransforms.isEmpty();
    }

    private int usages() {
      return dependentTransforms.size();
    }
  }

  /** Shared, mutable state during the translation of a pipeline. */
  public interface TranslationState {
    <T> Dataset<T> getDataset(PCollection<T> pCollection);

    <T> void putDataset(PCollection<T> pCollection, Dataset<T> dataset);

    MetricsContainerStepMap getMetrics();

    boolean isLeaf(PCollection<?> pCollection);

    LocalPipelineOptions getOptions();
  }

  private class EagerEvaluationVisitor extends PTransformVisitor implements TranslationState {
    private final Map<PCollection<?>, TranslationResult<?>> translationResults;
    private final LocalPipelineOptions options;
    private final Set<TranslationResult<?>> leaves;
    private final MetricsContainerStepMap metrics;

    public EagerEvaluationVisitor(
        LocalPipelineOptions options,
        MetricsContainerStepMap metrics,
        Map<PCollection<?>, TranslationResult<?>> translationResults) {
      this.translationResults = translationResults;
      this.options = options;
      this.metrics = metrics;
      this.leaves = new HashSet<>();
    }

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {

      AppliedPTransform<InT, OutT, PTransform<InT, OutT>> appliedTransform =
          (AppliedPTransform) node.toAppliedPTransform(getPipeline());
      try {
        translator.translate(transform, appliedTransform, this);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private <T> TranslationResult<T> getResult(PCollection<T> pCollection) {
      return (TranslationResult<T>) checkStateNotNull(translationResults.get(pCollection));
    }

    @Override
    public <T> Dataset<T> getDataset(PCollection<T> pCollection) {
      // FIXME remove once used
      return checkStateNotNull(getResult(pCollection).dataset);
    }

    @Override
    public <T> void putDataset(PCollection<T> pCollection, Dataset<T> dataset) {
      // If dataset is used multiple times or if it is a leaf, collect it into memory.
      // This is particularly necessary for leaves to trigger the evaluation in case of lazy
      // datasets.
      TranslationResult<T> res = getResult(pCollection);
      res.dataset = res.usages() > 1 || res.isLeaf() ? dataset.collect() : dataset;
    }

    @Override
    public MetricsContainerStepMap getMetrics() {
      return metrics;
    }

    @Override
    public boolean isLeaf(PCollection<?> pCollection) {
      return getResult(pCollection).isLeaf();
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
    private final Map<PCollection<?>, TranslationResult<?>> results = new HashMap<>();

    @Override
    <InT extends PInput, OutT extends POutput> void visit(
        Node node,
        PTransform<InT, OutT> transform,
        TransformTranslator<InT, OutT, PTransform<InT, OutT>> translator) {
      for (Map.Entry<TupleTag<?>, PCollection<?>> entry : node.getInputs().entrySet()) {
        TranslationResult<?> input = checkStateNotNull(results.get(entry.getValue()));
        input.dependentTransforms.add(transform);
      }
      for (PCollection<?> pOut : node.getOutputs().values()) {
        results.put(pOut, new TranslationResult<>(pOut));
      }
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
