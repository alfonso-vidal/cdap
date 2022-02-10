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

package io.cdap.cdap.internal.events;

import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ProgramStatusMetricsProviderTest extends AppFabricTestBase {

  private static ProgramStatusMetricsProvider metricsProvider;
  private final String MOCKED_RUN_ID = "6354a561-886c-11ec-8688-42010a8e0035";
  private final String MOCKED_ATTEMPT_ID = "1";
  private final String MOCKED_APPLICATION_JSON = "mocked_spark_application_response.json";
  private final String MOCKED_STAGES_JSON = "mocked_spark_stages_response.json";

  @BeforeClass
  public static void setupClass() throws IOException {
    metricsProvider = getInjector().getInstance(ProgramStatusMetricsProvider.class);
  }

  @Test
  public void testExtractAttemptId() {
    String responseStr = loadMockedResponseAsString(MOCKED_APPLICATION_JSON);
    String attemptId = metricsProvider.extractAttemptId(responseStr, MOCKED_RUN_ID);
    Assert.assertEquals(attemptId, MOCKED_ATTEMPT_ID);
  }

  @Test
  public void testExtractMetrics() {
    String responseStr = loadMockedResponseAsString(MOCKED_STAGES_JSON);
    ExecutionMetrics metrics = metricsProvider.extractMetrics(responseStr);
    Assert.assertEquals(metrics, getMockedMetrics());
  }

  private String loadMockedResponseAsString(String mockedFile) {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream responseIS = classLoader.getResourceAsStream(mockedFile);
    Assert.assertNotNull(responseIS);
    return new BufferedReader(new InputStreamReader(responseIS))
      .lines().collect(Collectors.joining(System.lineSeparator()));
  }

  private ExecutionMetrics getMockedMetrics() {
    return new ExecutionMetrics(
      10195,
      22,
      6046096,
      6237
    );
  }
}