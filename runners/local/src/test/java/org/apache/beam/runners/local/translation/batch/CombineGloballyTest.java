package org.apache.beam.runners.local.translation.batch;

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

  @Test
  public void testCombineGlobally() {
    Values<Integer> values = Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    PCollection<Integer> input = pipeline.apply(values).apply(Sum.integersGlobally());
    PAssert.that(input).containsInAnyOrder(55);
    pipeline.run();
  }
}
