/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.metrics.publisher;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Class that uses the Decorator pattern to wrap another {@link MetricsPublisher}.
 * This class limits the rate at which metrics are sent to the wrapped publisher.
 */
public class BufferedMetricsPublisher extends AbstractMetricsPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(BufferedMetricsPublisher.class);
  private BlockingQueue<MetricValues> metricsBuffer;
  private TimerService timerService;
  private final CConfiguration cConf;
  private final MetricsPublisher publisher;

  @Inject
  public BufferedMetricsPublisher(CConfiguration cConf, @Base MetricsPublisher publisher) {
    this.cConf = cConf;
    this.publisher = publisher;
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      LOG.debug("Metrics publisher is already initialized.");
      return;
    }
    LOG.info("Initializing BufferedMetricsPublisher.");
    int bufferCapacity = this.cConf.getInt(Constants.BufferedMetricsPublisher.BUFFER_CAPACITY);
    this.metricsBuffer = new ArrayBlockingQueue<>(bufferCapacity);
    int persistingFrequencySeconds =
      this.cConf.getInt(Constants.BufferedMetricsPublisher.PERSISTING_FREQUENCY_SECONDS);
    this.timerService = new TimerService(this, persistingFrequencySeconds);
    this.timerService.startAndWait();
    this.publisher.initialize();
  }

  private boolean isInitialized() {
    if (this.timerService == null || !this.timerService.isRunning()) {
      return false;
    }
    if (this.metricsBuffer == null) {
      return false;
    }
    return true;
  }

  @Override
  public void publish(Collection<MetricValues> metrics) {
    if (!isInitialized()) {
      throw new IllegalStateException("Initialize publisher before calling publish");
    }
    for (MetricValues metricValues : metrics) {
      boolean inserted = this.metricsBuffer.offer(metricValues);
      if (!inserted) {
        throw new IllegalStateException("Discarding metrics since queue is full");
      }
    }
    LOG.trace("Added {} MetricValues to buffer", metrics.size());
  }

  public int getRemainingCapacity() {
    if (!isInitialized()) {
      throw new IllegalStateException("Initialize publisher before getting buffer capacity");
    }
    return this.metricsBuffer.remainingCapacity();
  }

  public void drainBuffer() {
    if (!isInitialized()) {
      throw new IllegalStateException("Initialize publisher before draining buffer");
    }
    Collection<MetricValues> metrics = new ArrayList<>();
    this.metricsBuffer.drainTo(metrics);
    try {
      this.publisher.publish(metrics);
    } catch (Exception e) {
      LOG.warn("Error while persisting metrics.", e);
      return;
    }
    LOG.trace("Drained {} metrics from buffer. Remaining capacity {}.",
              metrics.size(), this.metricsBuffer.remainingCapacity());
  }

  @Override
  public void close() throws IOException {
    this.metricsBuffer.clear();
    if (this.timerService.isRunning()) {
      this.timerService.stopAndWait();
    }
    this.publisher.close();
    LOG.info("BufferedMetricsPublisher is closed.");
  }

  @Qualifier
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface Base {
  }

  /**
   * Service that calls method in a {@link BufferedMetricsPublisher} to drain it's buffer at
   * regular intervals.
   */
  private static class TimerService extends AbstractScheduledService {
    private final int frequencySeconds;
    private ScheduledExecutorService executor;
    private final BufferedMetricsPublisher publisher;

    TimerService(BufferedMetricsPublisher publisher, int frequencySeconds) {
      this.publisher = publisher;
      this.frequencySeconds = frequencySeconds;
    }

    @Override
    protected void runOneIteration() {
      publisher.drainBuffer();
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(this.frequencySeconds, this.frequencySeconds, TimeUnit.SECONDS);
    }

    @Override
    protected final ScheduledExecutorService executor() {
      executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("timer-service"));
      return executor;
    }

    @Override
    protected void shutDown() throws IOException {
      if (executor != null) {
        executor.shutdownNow();
      }
      LOG.info("Shutting down TimerService has completed.");
    }
  }
}
