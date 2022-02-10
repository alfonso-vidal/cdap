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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.StreamSupport;
import javax.inject.Inject;

public class ProgramStatusMetricsProvider implements MetricsProvider {

  //Pending on actual location
  private final String SPARK_BASE_URL_CONFIGURATION = "";
  private final String SPARK_APPLICATIONS_ENDPOINT = "/api/v1/applications";
  private final int MAX_RETIRES = 3;

  private final Logger logger = LoggerFactory.getLogger(ProgramStatusMetricsProvider.class);
  private final JsonParser jsonParser = new JsonParser();

  private final CConfiguration cConf;
  private final HttpRequestConfig httpRequestConfig;

  @Inject
  public ProgramStatusMetricsProvider(CConfiguration cConf) {
    int connectionTimeout = cConf.getInt(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    int readTimeout = cConf.getInt(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);
    httpRequestConfig = new HttpRequestConfig(connectionTimeout, readTimeout, false);

    this.cConf = cConf;
  }

  @Override
  public ExecutionMetrics retrieveMetrics(ProgramRunId runId) {
    ExecutionMetrics metrics = null;
    int retriesCount = 0;
    String runIdStr = runId.getRun();
    String sparkHistoricBaseURL = cConf.get(SPARK_BASE_URL_CONFIGURATION);
    String applicationsURL = String.format("%s%s", sparkHistoricBaseURL,
                                           SPARK_APPLICATIONS_ENDPOINT);
    while (Objects.isNull(metrics) && retriesCount < MAX_RETIRES) {
      HttpResponse applicationResponse;
      applicationResponse = doGetWithRetries(applicationsURL);
      String attemptId = extractAttemptId(applicationResponse.getResponseBodyAsString(), runIdStr);
      if (Objects.nonNull(attemptId)) {
        HttpResponse stagesResponse;
        String stagesURL = String.format("%s/%s/%s/%s/stages", sparkHistoricBaseURL,
                                         SPARK_APPLICATIONS_ENDPOINT, runIdStr, attemptId);
        stagesResponse = doGetWithRetries(stagesURL);
        metrics = extractMetrics(stagesResponse.getResponseBodyAsString());
      }
      if (Objects.isNull(metrics)) {
        retriesCount++;
      }
    }

    //Won't able to retrieve metrics if the object is null so returns an empty metrics object
    if (Objects.isNull(metrics)) {
      metrics = ExecutionMetrics.emptyMetrics();
    }
    return metrics;
  }

  private HttpResponse doGetWithRetries(String url) {
    int retriesCont = 0;
    HttpResponse response = null;
    while (Objects.isNull(response) && (retriesCont < MAX_RETIRES)) {
      try {
        response = doGet(url);
        if (response.getResponseCode() != 200) {
          response = null;
          retriesCont++;
        }
      } catch (IOException e) {
        logger.error("Error during retry number " + (retriesCont + 1), e);
        retriesCont++;
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        logger.error("Error during retrying sleep", e);
      }
    }

    if (Objects.isNull(response)) {
      throw new RuntimeException("Error requesting for URL [" + url + "] after "
                                   + retriesCont + " retries.");
    }

    return response;
  }

  private HttpResponse doGet(String url) throws IOException {
    URL requestURL = new URL(url);
    HttpRequest httpRequest = HttpRequest.get(requestURL).build();
    return HttpRequests.execute(httpRequest, httpRequestConfig);
  }

  @VisibleForTesting
  protected String extractAttemptId(String responseBody, String runId) {
    final String[] attemptId = new String[1];
    JsonArray jsonAppResponse = jsonParser.parse(responseBody)
      .getAsJsonArray();

    StreamSupport.stream(jsonAppResponse.spliterator(), false)
      .filter(app -> runId.equals(app.getAsJsonObject().get("id").getAsString()))
      .findFirst().ifPresent(app -> {
        JsonArray attArray = app.getAsJsonObject().get("attempts").getAsJsonArray();
        StreamSupport.stream(attArray.spliterator(), false)
          .sorted(Comparator.comparingLong(el -> el.getAsJsonObject().get("endTimeEpoch").getAsLong()))
          .filter(attempt -> attempt.getAsJsonObject().get("completed").getAsBoolean())
          .findFirst().ifPresent(
            element -> attemptId[0] = element.getAsJsonObject().get("attemptId").getAsString()
          );
      });

    return attemptId[0];
  }

  @VisibleForTesting
  protected ExecutionMetrics extractMetrics(String responseBody) {
    JsonArray jsonStagesResponse = jsonParser.parse(responseBody)
      .getAsJsonArray();

    return StreamSupport.stream(jsonStagesResponse.spliterator(), false)
      .filter(stage -> "COMPLETED".equals(stage.getAsJsonObject().get("status").getAsString()))
      .map(stage -> new ExecutionMetrics(
        stage.getAsJsonObject().get("inputRecords").getAsInt(),
        stage.getAsJsonObject().get("outputRecords").getAsInt(),
        stage.getAsJsonObject().get("inputBytes").getAsLong(),
        stage.getAsJsonObject().get("outputBytes").getAsLong()
      )).findFirst().orElseGet(ExecutionMetrics::nullMetrics);
  }
}
