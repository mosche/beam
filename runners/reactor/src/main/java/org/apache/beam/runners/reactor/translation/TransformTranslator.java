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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.reactor.LocalPipelineOptions;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;

@Internal
public abstract class TransformTranslator<
    InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>> {

  protected abstract void translate(TransformT transform, Context cxt) throws Exception;

  final void translate(
      TransformT transform,
      AppliedPTransform<InT, OutT, TransformT> appliedTransform,
      PipelineTranslator.TranslationState translationState)
      throws Exception {
    translate(transform, new Context(appliedTransform, translationState));
  }

  /**
   * Checks if a composite / primitive transform can be translated. Composites that cannot be
   * translated as is, will be exploded further for translation of their parts.
   *
   * <p>This returns {@code true} by default and should be overridden where necessary.
   *
   * @throws RuntimeException If a transform uses unsupported features, an exception shall be thrown
   *     to give early feedback before any part of the pipeline is run.
   */
  protected boolean canTranslate(TransformT transform) {
    return true;
  }

  /**
   * Available mutable context to translate a {@link PTransform}. The context is backed by the
   * shared {@link PipelineTranslator.TranslationState} of the {@link PipelineTranslator}.
   */
  protected class Context implements PipelineTranslator.TranslationState {
    private final AppliedPTransform<InT, OutT, TransformT> transform;
    private final PipelineTranslator.TranslationState state;

    private Context(
        AppliedPTransform<InT, OutT, TransformT> transform,
        PipelineTranslator.TranslationState state) {
      this.transform = transform;
      this.state = state;
    }

    public InT getInput() {
      return (InT) getOnlyElement(TransformInputs.nonAdditionalInputs(transform));
    }

    public MetricsContainer getMetricsContainer() {
      return state.getMetrics().getContainer(transform.getFullName());
    }

    @Override
    public MetricsContainerStepMap getMetrics() {
      return state.getMetrics();
    }

    public Map<TupleTag<?>, PCollection<?>> getInputs() {
      return transform.getInputs();
    }

    public Map<TupleTag<?>, PCollection<?>> getOutputs() {
      return transform.getOutputs();
    }

    public OutT getOutput() {
      return (OutT) getOnlyElement(transform.getOutputs().values());
    }

    public <T> PCollection<T> getOutput(TupleTag<T> tag) {
      return checkStateNotNull(
          (PCollection<T>) transform.getOutputs().get(tag), "Invalid tag %", tag);
    }

    public AppliedPTransform<InT, OutT, TransformT> getCurrentTransform() {
      return transform;
    }

    @Override
    public <T> void provide(PCollection<T> pCollection, Dataset<T, ?> dataset) {
      state.provide(pCollection, dataset);
    }

    @Override
    public <T> Dataset<T, ?> require(PCollection<T> pCollection) {
      return state.require(pCollection);
    }

    @Override
    public <T1, T2> void translate(PCollection<T2> pCollection, Translation<T1, T2> translation) {
      state.translate(pCollection, translation);
    }

    @Override
    public boolean isLeaf(PCollection<?> pCollection) {
      return state.isLeaf(pCollection);
    }

    @Override
    public LocalPipelineOptions getOptions() {
      return state.getOptions();
    }
  }
}
