package org.apache.beam.runners.local.translation.metrics;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

import java.io.Closeable;
import java.io.IOException;

public class DoFnRunnerWithMetrics<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  private final DoFnRunner<InputT, OutputT> delegate;
  private final MetricsContainer metrics;

  public DoFnRunnerWithMetrics(DoFnRunner<InputT, OutputT> delegate, MetricsContainer metrics) {
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return delegate.getFn();
  }

  @Override
  public void startBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metrics)) {
      delegate.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(final WindowedValue<InputT> elem) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metrics)) {
      delegate.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <KeyT> void onTimer(
      final String timerId,
      final String timerFamilyId,
      KeyT key,
      final BoundedWindow window,
      final Instant timestamp,
      final Instant outputTimestamp,
      final TimeDomain timeDomain) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metrics)) {
      delegate.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metrics)) {
      delegate.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    delegate.onWindowExpiration(window, timestamp, key);
  }
}
