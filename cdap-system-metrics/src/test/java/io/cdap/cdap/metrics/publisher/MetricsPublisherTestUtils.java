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

import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;

/**
 * Class that provides utility function to help to test
 * {@link io.cdap.cdap.api.metrics.MetricsPublisher}s
 */
class MetricsPublisherTestUtils {

  /**
   * @param num Number of fake {@link MetricValues} to get. The metric values will
   *            have increasing timestamps.
   * @return A {@link Collection} of {@link MetricValues} objects.
   */
  static Collection<MetricValues> getMockMetricValuesArray(int num) {
    MetricValue metric = new MetricValue("mock.metric", MetricType.COUNTER, 1);
    Collection<MetricValues> metricValues = new ArrayList<>();
    for (int mockTime = 0; mockTime < num; ++mockTime) {
      metricValues.add(
        new MetricValues(new TreeMap<>(), metric.getName(), mockTime, metric.getValue(), metric.getType()));
    }
    return metricValues;
  }

  /**
   * @param num Number of fake {@link MetricValue} to get.
   * @return A {@link Collection} of fake {@link MetricValue} of requested length.
   */
  static Collection<MetricValue> getMockMetricArray(int num) {
    MetricValue metric = new MetricValue("mock.metric", MetricType.COUNTER, 1);
    return new ArrayList<>(Collections.nCopies(num, metric));
  }
}
