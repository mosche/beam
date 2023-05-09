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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class CombineGloballyTest {

  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(TestOptions.create());

  private PCollection<Integer> summedValues() {
    Values<Integer> values = Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    return pipeline.apply(values).apply(Sum.integersGlobally());
  }

  @Test
  public void testCombineGlobally() {
    PCollection<Integer> input = summedValues();
    PAssert.that(input).containsInAnyOrder(55);
    pipeline.run();
  }

  @Test
  public void testCombineGloballyWithMultipleSubscribers() {
    PCollection<Integer> input = summedValues();
    PAssert.that(input).containsInAnyOrder(55);
    PAssert.that(input).containsInAnyOrder(55);
    pipeline.run();
  }
}
