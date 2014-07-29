package gcsbuddy;

public interface RetryStrategy {
  boolean shouldRetry();
  long nextRetryIntervalInMs();
}
