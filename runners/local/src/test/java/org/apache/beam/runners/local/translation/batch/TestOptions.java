package org.apache.beam.runners.local.translation.batch;

import org.apache.beam.runners.local.LocalPipelineOptions;
import org.apache.beam.runners.local.LocalRunner;
import org.apache.beam.sdk.testing.TestPipeline;

class TestOptions {
  private TestOptions(){}

  static LocalPipelineOptions create(){
    LocalPipelineOptions opts =
        TestPipeline.testingPipelineOptions().as(LocalPipelineOptions.class);
    opts.setRunner(LocalRunner.class);
    opts.setMetricsEnabled(true);
    opts.setSplits(1);
    return opts;
  }
}
