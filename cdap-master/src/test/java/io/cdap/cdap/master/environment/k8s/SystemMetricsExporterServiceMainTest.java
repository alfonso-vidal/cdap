/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SystemMetricsExporterServiceMain}.
 */
public class SystemMetricsExporterServiceMainTest {

  @Test
  public void testGetComponentName() {
    // Ensure valid pod names are parsed correctly
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "instance-abc", "cdap-instance-abc-appfabric-0"),
      "appfabric-0");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "123", "cdap-123-appfabric-23"),
      "appfabric-23");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "cdap", "cdap-cdap-appfabric-0"),
      "appfabric-0");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        null, "cdap-new-name-format"),
      "new-name-format");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        null, "new-name-format"),
      "new-name-format");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "", "cdap-test-metrics-0"),
      "test-metrics-0");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "test-cdap", "cdap-test-cdap-preview-runner-b5786a15-e8f4-47-0cebad7d67-0"),
      "preview-runner-b5786a15-e8f4-47-0cebad7d67-0");
    Assert.assertEquals(
      SystemMetricsExporterServiceMain.getComponentName(
        "dap", "cdap-dap-preview-runner-b5786a15-e8f4-47-0cebad7d67-0"),
      "preview-runner-b5786a15-e8f4-47-0cebad7d67-0");
  }
}
