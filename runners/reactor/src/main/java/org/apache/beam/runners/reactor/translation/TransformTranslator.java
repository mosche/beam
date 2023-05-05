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

  protected abstract void translate(Context<InT, OutT, TransformT> cxt);

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
   * Context for transforming a PTransform.
   *
   * <p>Note, the context is only available during translation.
   */
  public interface Context<
      InT extends PInput, OutT extends POutput, TransformT extends PTransform<InT, OutT>> {
    <T> void provide(PCollection<T> pCollection, Dataset<T, ?> dataset);

    <T> Dataset<T, ?> require(PCollection<T> pCollection);

    <T1, T2> void translate(PCollection<T2> pCollection, Translation<T1, T2> translation);

    boolean isLeaf(PCollection<?> pCollection);

    MetricsContainer getMetricsContainer();

    LocalPipelineOptions getOptions();

    AppliedPTransform<InT, OutT, TransformT> getAppliedTransform();

    default TransformT getTransform() {
      return getAppliedTransform().getTransform();
    }

    default Map<TupleTag<?>, PCollection<?>> getInputs() {
      return getAppliedTransform().getInputs();
    }

    default Map<TupleTag<?>, PCollection<?>> getOutputs() {
      return getAppliedTransform().getOutputs();
    }

    default InT getInput() {
      return (InT) getOnlyElement(TransformInputs.nonAdditionalInputs(getAppliedTransform()));
    }

    default OutT getOutput() {
      return (OutT) getOnlyElement(getAppliedTransform().getOutputs().values());
    }

    default <T> PCollection<T> getOutput(TupleTag<T> tag) {
      return checkStateNotNull(
          (PCollection<T>) getAppliedTransform().getOutputs().get(tag), "Invalid tag %", tag);
    }
  }
}
