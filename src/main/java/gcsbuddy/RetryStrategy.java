package gcsbuddy;

public interface RetryStrategy {
  boolean retriesRemaining();
  long nextRetryIntervalInMs();
}
