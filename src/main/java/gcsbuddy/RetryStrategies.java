package gcsbuddy;

import com.google.common.base.Objects;

import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("UnusedDeclaration")
public final class RetryStrategies {

  /**
   * Uses exponential backoff (http://en.wikipedia.org/wiki/Exponential_backoff) with an optional random amount of
   * "jitter", to prevent multiple instances from using the exact same retry intervals.  The jitter, if used, is
   * expressed as a percentage, and is chosen randomly as a positive or negative delta from the backoff amount.
   *
   * In other words:
   *    N^0 +/- jitter, N^1 +/- jitter, ...,  N^n +/- jitter
   *    where N is the supplied base and jitter is a % of the current N^n term
   */
  private static class ExponentialBackoff implements RetryStrategy {

    protected static final long DEFAULT_INITIAL_RETRY_INTERVAL = 100L;

    // The base b to use in the b^n exponential backoff calculation.
    protected static final int DEFAULT_BASE = 2;

    // Each retry interval will have a random delta +/- N% added.
    protected static final double DEFAULT_JITTER_PERCENTAGE = 0.0D;

    protected final long initialRetryIntervalInMs;
    protected final int maxNumberOfRetries;
    protected final int exponent;
    protected final double jitterPercentage;

    protected final Random rand;
    protected long currentRetryIntervalInMs;
    protected int retriesRemaining;

    ExponentialBackoff(final int maxNumberOfRetries) {
      this(maxNumberOfRetries, DEFAULT_INITIAL_RETRY_INTERVAL, DEFAULT_BASE, DEFAULT_JITTER_PERCENTAGE);
    }

    ExponentialBackoff(final int maxNumberOfRetries, final long initialRetryIntervalInMs) {
      this(maxNumberOfRetries, initialRetryIntervalInMs, DEFAULT_BASE, DEFAULT_JITTER_PERCENTAGE);
    }

    ExponentialBackoff(final int maxNumberOfRetries,
                       final long initialRetryIntervalInMs,
                       final int exponent,
                       final double jitterPercentage) {
      checkArgument(initialRetryIntervalInMs >= 0, "initialRetryIntervalInMs must be >= 0");
      checkArgument(maxNumberOfRetries > 0,
                    "maxRetryIntervalInMs must be > 0");
      checkArgument(exponent > 0, "exponent must be > 0");
      checkArgument(jitterPercentage >= 0D && jitterPercentage <= 1D,
                    "jitterPercentage must be in [0.0, 1.0]");
      this.initialRetryIntervalInMs = initialRetryIntervalInMs;
      this.maxNumberOfRetries = maxNumberOfRetries;
      this.exponent = exponent;
      this.currentRetryIntervalInMs = initialRetryIntervalInMs;
      this.jitterPercentage = jitterPercentage;
      this.rand = new Random(System.currentTimeMillis());
      this.retriesRemaining = maxNumberOfRetries;
    }

    @Override
    public boolean retriesRemaining() {
      return retriesRemaining > 0;
    }

    @Override
    public long nextRetryIntervalInMs() {

      --retriesRemaining;

      // chooses a number that +/- some factor:
      //  N^0 +/- delta, N^1 +/- delta, ...,  N^n +/- delta
      //   where N is the supplied base and delta is a % of the current N^n term
      double delta = jitterPercentage * currentRetryIntervalInMs;
      double minInterval = currentRetryIntervalInMs - delta;
      double maxInterval = currentRetryIntervalInMs + delta;

      long result = (long) (minInterval + (rand.nextDouble() * (maxInterval - minInterval)));

      currentRetryIntervalInMs *= exponent;

      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("retries", this.maxNumberOfRetries)
        .add("exponent", this.exponent)
        .add("initial interval(ms)", this.initialRetryIntervalInMs)
        .add("jitter %", this.jitterPercentage)
        .toString();
    }
  }

  /**
   * Uses exponential backoff (http://en.wikipedia.org/wiki/Exponential_backoff) to choose a range from which a random
   * retry interval is chosen.
   *
   * In other words, for a base of N, choose a random retry interval in the ranges: [0, N^0), [0, N^1), ..., [0, N^n)
   */
  private static class RandomizedExponentialBackoff implements RetryStrategy {

    protected static final long DEFAULT_INITIAL_RETRY_INTERVAL = 100L;
    protected static final int DEFAULT_BASE = 2;
    protected static final double DEFAULT_RANDOMIZATION_PERCENTAGE = 1.0D;

    protected final long initialRetryIntervalInMs;
    protected final int maxNumberOfRetries;
    protected final int exponent;
    protected final double randomizationPercentage;
    protected final Random rand;
    protected long currentRetryIntervalInMs;
    protected int retriesRemaining;

    RandomizedExponentialBackoff(final int maxNumberOfRetries) {
      this(maxNumberOfRetries, DEFAULT_INITIAL_RETRY_INTERVAL, DEFAULT_BASE, DEFAULT_RANDOMIZATION_PERCENTAGE);
    }

    RandomizedExponentialBackoff(final int maxNumberOfRetries, final long initialRetryIntervalInMs) {
      this(maxNumberOfRetries, initialRetryIntervalInMs, DEFAULT_BASE, DEFAULT_RANDOMIZATION_PERCENTAGE);
    }

    RandomizedExponentialBackoff(final int maxNumberOfRetries,
                                 final long initialRetryIntervalInMs,
                                 final int exponent,
                                 final double randomizationPercentage) {
      checkArgument(initialRetryIntervalInMs >= 0, "initialRetryIntervalInMs must be >= 0");
      checkArgument(maxNumberOfRetries > 0,
                    "maxNumberOfRetries must be > 0");
      checkArgument(exponent > 0, "exponent must be > 0");
      checkArgument(randomizationPercentage > 0D && randomizationPercentage <= 1D,
                    "randomizationPercentage must be in (0.0, 1.0]");
      this.initialRetryIntervalInMs = initialRetryIntervalInMs;
      this.maxNumberOfRetries = maxNumberOfRetries;
      this.exponent = exponent;
      this.currentRetryIntervalInMs = initialRetryIntervalInMs;
      this.randomizationPercentage = randomizationPercentage;
      this.rand = new Random(System.currentTimeMillis());
      this.retriesRemaining = maxNumberOfRetries;
    }

    @Override
    public boolean retriesRemaining() {
      return retriesRemaining > 0;
    }

    @Override
    public long nextRetryIntervalInMs() {

      --retriesRemaining;

      // chooses a random number between [0, N^0), [0, N^1), then [0, N^2), ... , [0, N^n)
      double delta = randomizationPercentage * currentRetryIntervalInMs;
      long result = (long) (rand.nextDouble() * delta);

      currentRetryIntervalInMs *= exponent;

      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("retries", this.maxNumberOfRetries)
        .add("exponent", this.exponent)
        .add("initial interval(ms)", this.initialRetryIntervalInMs)
        .add("randomization %", this.randomizationPercentage)
        .toString();
    }
  }

  /**
   * Retries N number of times, using a fixed retry interval.
   */
  private static class RetryNTimes implements RetryStrategy {

    protected static final long DEFAULT_RETRY_INTERVAL = 100L;

    protected final int numberOfRetries;
    protected final long retryIntervalInMs;
    protected int retriesRemaining;

    RetryNTimes(int numberOfRetries) {
      this(numberOfRetries, DEFAULT_RETRY_INTERVAL);
    }

    RetryNTimes(int numberOfRetries, long retryIntervalInMs) {
      checkArgument(numberOfRetries > 0, "numberOfRetries must be > 0");
      checkArgument(retryIntervalInMs >= 0, "retryIntervalInMs must be >= 0");
      this.numberOfRetries = numberOfRetries;
      this.retriesRemaining = numberOfRetries;
      this.retryIntervalInMs = retryIntervalInMs;
    }

    @Override
    public boolean retriesRemaining() {
      return retriesRemaining > 0;
    }

    @Override
    public long nextRetryIntervalInMs() {
      this.retriesRemaining--;
      return this.retryIntervalInMs;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("retries", this.numberOfRetries)
        .add("interval(ms)", this.retryIntervalInMs)
        .toString();
    }
  }

  /**
   * Does exactly what the name implies: doesn't retry
   */
  private static class NoRetry implements RetryStrategy {

    private static final NoRetry INSTANCE = new NoRetry();

    private NoRetry() {}

    /** Returns the only instance of this class. */
    public static NoRetry get() { return INSTANCE; }

    @Override
    public boolean retriesRemaining() {
      return false;
    }

    @Override
    public long nextRetryIntervalInMs() {
      return 0;
    }
  }

  public static RetryStrategy exponentialBackoffWithJitter(final int maxNumRetries) {
    return new ExponentialBackoff(maxNumRetries);
  }

  public static RetryStrategy exponentialBackoffWithJitter(final int maxNumRetries, final long initialRetryIntervalInMs) {
    return new ExponentialBackoff(maxNumRetries, initialRetryIntervalInMs);
  }

  public static RetryStrategy randomizedExponentialBackoff(final int maxNumRetries) {
    return new RandomizedExponentialBackoff(maxNumRetries);
  }

  public static RetryStrategy randomizedExponentialBackoff(final int maxNumRetries, final long initialRetryIntervalInMs) {
    return new RandomizedExponentialBackoff(maxNumRetries, initialRetryIntervalInMs);
  }

  public static RetryStrategy nTimes(final int n) {
    return new RetryNTimes(n, 100L);
  }

  public static RetryStrategy nTimes(final int n, final long retryIntervalInMs) {
    return new RetryNTimes(n, retryIntervalInMs);
  }

  public static RetryStrategy noRetry() {
    return NoRetry.get();
  }


}
