package io.cdap.cdap.internal.events.http;

public class SparkApplicationsResponse {
  private String id;
  private String name;
  private Attempt[] attempts;

  public SparkApplicationsResponse() {
  }

  public Attempt[] getAttempts() {
    return attempts;
  }

  public String getId() {
    return id;
  }

  public static class Attempt {
    private String attemptId;
    private String startTime;
    private String endTime;
    private String lastUpdated;
    private int duration;
    private String sparkUser;
    private boolean completed;
    private String appSparkVersion;
    private long startTimeEpoch;
    private long endTimeEpoch;
    private long lastUpdatedEpoch;

    public Attempt() {
    }

    public String getAttemptId() {
      return attemptId;
    }

    public long getEndTimeEpoch() {
      return endTimeEpoch;
    }

    public boolean isCompleted() {
      return completed;
    }
  }
}
