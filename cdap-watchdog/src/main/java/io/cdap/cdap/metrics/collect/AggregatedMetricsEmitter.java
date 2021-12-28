/*
 * Copyright © 2014 Cask Data, Inc.
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
package io.cdap.cdap.metrics.collect;

import com.google.common.primitives.Doubles;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link MetricsEmitter} that aggregates  values for a metric
 * during collection and emit the aggregated value when emit.
 */
final class AggregatedMetricsEmitter implements MetricsEmitter {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsEmitter.class);

  private final String name;
  // metric value
  private final AtomicLong value;

  private volatile MetricType metricType = MetricType.COUNTER;

  private ArrayList<Double> values = new ArrayList<>();

  AggregatedMetricsEmitter(String name) {
    if (name == null || name.isEmpty()) {
      LOG.warn("Creating emmitter with " + (name == null ? "null" : "empty") + " name, ");
    }

    this.name = name;
    this.value = new AtomicLong();
  }

  void increment(long value) {
    this.value.addAndGet(value);
  }


  @Override
  public MetricValue emit() {
    // todo CDAP-2195 - potential race condition , reseting value and type has to be done together
    if (metricType == MetricType.DISTRIBUTION) {
      ArrayList<Double> valuesToBeEmitted;
      synchronized (this) {
        valuesToBeEmitted = values;
        values = new ArrayList<>();
      }

      MetricValue returnValue = new MetricValue(name, Doubles.toArray(valuesToBeEmitted));
      return returnValue;
    } else {
      long value = this.value.getAndSet(0);
      return new MetricValue(name, metricType, value);
    }
  }

  public void gauge(long value) {
    this.value.set(value);
    this.metricType = MetricType.GAUGE;
  }

  public void distribution(double value) {
    synchronized (this) {
      values.add(value);
    }
    /*
    if (bucketCounts == null) {
      bucketCounts = new long[MetricValue.RESPONSE_TIME_BOUNDS_MS.length + 1];
      Arrays.fill(bucketCounts, 0);
      this.metricType = MetricType.DISTRIBUTION;
    }

    // TODO make logic more general and not hardcode 10
    int bucket = 0;
    double residue = value;
    while (residue > 10 && bucket < MetricValue.RESPONSE_TIME_BOUNDS_MS.length) {
      bucket++;
      residue = residue / 10;
    }

    bucketCounts[bucket] = bucketCounts[bucket] + 1;

     */
  }


}
