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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class ParDoTest {
  @Rule public transient TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  PCollection<Integer> plusOne() {
    return pipeline
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
        .apply("+1", ParDo.of(PLUS_ONE_DOFN));
  }

  @Test
  public void testPardo() {
    PCollection<Integer> input = plusOne();
    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    pipeline.run();
  }

  @Test
  public void testPardoWithMultipleSubscribers() {
    PCollection<Integer> input = plusOne();
    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
    pipeline.run();
  }

  @Test
  public void testPardoWithUnusedAdditionalOutput() {
    PCollectionTuple outputs =
        plusOne().apply("even", ParDo.of(EVEN_DOFN).withOutputTags(EVEN, TupleTagList.of(UNEVEN)));

    PAssert.that(outputs.get(EVEN)).containsInAnyOrder(2, 4, 6, 8, 10);
    pipeline.run();
  }

  @Test(expected = Pipeline.PipelineExecutionException.class)
  public void testPardoUserException() {
    pipeline.apply(Create.of(1)).apply(ParDo.of(THROWING_DOFN));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTwoPardoInRow() {
    PCollection<Integer> input = plusOne().apply("Plus 1 (2nd)", ParDo.of(PLUS_ONE_DOFN));
    PAssert.that(input).containsInAnyOrder(3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    pipeline.run();
  }

  @Test
  public void testSideInputAsList() {
    PCollectionView<List<Integer>> sideInput =
        pipeline.apply("Create sideInput", Create.of(1, 2, 3)).apply(View.asList());
    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(ParDo.of(new DiscardFromListSideIn(sideInput)).withSideInputs(sideInput));
    PAssert.that(input).containsInAnyOrder(4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  private static class DiscardFromSingletonSideIn extends DoFn<Integer, Integer> {
    private final PCollectionView<Integer> sideInput;

    DiscardFromSingletonSideIn(PCollectionView<Integer> sideInput) {
      this.sideInput = sideInput;
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
      Integer sideInputValue = c.sideInput(sideInput);
      if (!sideInputValue.equals(c.element())) {
        c.output(c.element());
      }
    }
  }

  @Test
  public void testSideInputAsSingleton() {
    PCollectionView<Integer> sideInput =
        pipeline.apply("Create sideInput", Create.of(1)).apply(View.asSingleton());

    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(ParDo.of(new DiscardFromSingletonSideIn(sideInput)).withSideInputs(sideInput));

    PAssert.that(input).containsInAnyOrder(2, 3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  @Test
  public void testSideInputAsMap() {
    PCollectionView<Map<String, Integer>> sideInput =
        pipeline
            .apply("Create sideInput", Create.of(KV.of("key1", 1), KV.of("key2", 2)))
            .apply(View.asMap());
    PCollection<Integer> input =
        pipeline
            .apply("Create input", Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .apply(ParDo.of(new DiscardFromMapSideIn(sideInput)).withSideInputs(sideInput));
    PAssert.that(input).containsInAnyOrder(3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run();
  }

  private static final DoFn<Integer, Integer> THROWING_DOFN =
      new DoFn<Integer, Integer>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          throw new RuntimeException("Got: " + c.element());
        }
      };

  private static final DoFn<Integer, Integer> PLUS_ONE_DOFN =
      new DoFn<Integer, Integer>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          c.output(c.element() + 1);
        }
      };

  private static final TupleTag<Integer> EVEN = new TupleTag<Integer>() {};
  private static final TupleTag<String> UNEVEN = new TupleTag<String>() {};
  private static final DoFn<Integer, Integer> EVEN_DOFN =
      new DoFn<Integer, Integer>() {
        @ProcessElement
        public void processElement(@Element Integer i, MultiOutputReceiver out) {
          if (i % 2 == 0) {
            out.get(EVEN).output(i);
          } else {
            out.get(UNEVEN).output(i.toString());
          }
        }
      };

  private static class DiscardFromListSideIn extends DoFn<Integer, Integer> {
    private final PCollectionView<List<Integer>> sideInput;

    public DiscardFromListSideIn(PCollectionView<List<Integer>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<Integer> sideInputValue = c.sideInput(sideInput);
      if (!sideInputValue.contains(c.element())) {
        c.output(c.element());
      }
    }
  }

  private static class DiscardFromMapSideIn extends DoFn<Integer, Integer> {
    private final PCollectionView<Map<String, Integer>> sideInput;

    public DiscardFromMapSideIn(PCollectionView<Map<String, Integer>> sideInput) {
      this.sideInput = sideInput;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<String, Integer> sideInputValue = c.sideInput(sideInput);
      if (!sideInputValue.containsKey("key" + c.element())) {
        c.output(c.element());
      }
    }
  }
}
