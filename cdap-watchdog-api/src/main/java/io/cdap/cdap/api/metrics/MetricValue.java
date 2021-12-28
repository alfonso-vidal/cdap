/*
 * Copyright 2015 Cask Data, Inc.
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
package io.cdap.cdap.api.metrics;

/**
 * Carries the "raw" emitted metric data point: metric name, type, and value
 */
public class MetricValue {

  private String name;
  private MetricType type;
  private long value;
  public static final double[] RESPONSE_TIME_BOUNDS_MS = {10, 100, 1000, 10000, 100000};

  /**
   * Based on https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TypedValue#Distribution
   *
   * If N is the size of array returned by getBounds(), the underflow bucket has number 0;
   * the finite buckets, if any, have numbers 1 through N-2
   * and the overflow bucket has number N-1. The size of bucketCounts must not be greater than N.
   */
  private long[] bucketCounts;

  /**
   * Based on https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TypedValue#Explicit
   *
   * Specifies a set of buckets with arbitrary widths. Size of bounds has to be at least 1.
   *
   * There are size(bounds) + 1 (= N) buckets. Bucket i has the following boundaries:
   */
  private double[] values;

  public MetricValue (String name, MetricType type, long value) {
    // TODO: only for gauge and increment
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public MetricValue(String name, double[] values) {
    this.name = name;
    this.type = MetricType.DISTRIBUTION;
    this.values = values;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public long getValue() {
    return value;
  }

  public double[] getValues() {
    return values;
  }

  @Override
  public String toString() {
    // TODO fix to handle distribution
    return "MetricValue{" +
      "name='" + name + '\'' +
      ", type=" + type +
      ", value=" + value +
      '}';
  }
}
