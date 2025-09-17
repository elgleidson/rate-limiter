package com.github.elgleidson.dsa.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class FastLeakyBucketRateLimiter implements RateLimiter {

  // 100 requests / min => 100 / 60_000 ms
  // Using floating points that will give us:
  // - limitPerWindow = 100
  // - leakRatePerMs = 0.00166667 requests / ms
  //
  // After 100ms that gives us 0.166667 request.
  // After 600ms that gives us 1 whole request.
  //
  // Using long scale to avoid floating point arithmetic that will give us:
  // - limitPerWindow = 100_000_000 micro-requests
  // - leakRatePerMs = 1666 micro-requests / ms, same as the first 6 decimal places of our floating point impl = 0.001666 * scale
  //
  // After 100ms that gives us 166_600 micro-requests
  // After 600ms that gives us 999_600 micro-requests (a little less than 1 whole token)
  // After 601ms that gives us 1_001_266 micro-requests (a little more than 1 whole token)
  //
  // It's a trade-off:
  // - Floating point is more precise, but more costly, and we can have rounding issues.
  // - Long scale is faster, no rounding issues, but it's not as precise. Increasing scale could lead to math overflow.
  private static final int SCALE = 1_000_000;

  private final long limitPerWindow;
  private final long leakRatePerMs;
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public FastLeakyBucketRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  FastLeakyBucketRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.limitPerWindow = limitPerWindow * SCALE;
    this.leakRatePerMs = this.limitPerWindow / windowSizeInMs;
    this.requests = new ConcurrentHashMap<>();
    this.currentTimeInMsSupplier = currentTimeInMsSupplier;
  }

  @Override
  public boolean isAllowed(String identifier) {
    long now = currentTimeInMsSupplier.getAsLong();
    // to scape the lambda scope inside compute()
    // this variable is per thread local, so it's thread safe
    // can't be added to Hits as it would make no thread-safe: A and B for the same identifier
    // A enters compute(), change Hits.allowed = true
    // B waits
    // A leaves computer()
    // B enters compute(), change Hits.allowed = false before A reads it, so A reads as false, instead of true.
    final boolean[] allowed = new boolean[1];
    requests.compute(identifier, (key, existing) -> {
      // Hits is mutable on purpose to avoid object allocation / CG every time.
      // So we only create Hits once, and then we keep updating it accordingly.
      // It doesn't stress the CG, so it's more performant.
      // No issue as compute() is atomic / thread-safe per key.
      if (existing == null) {
        existing = new Hits(now, 0);
      } else {
        existing.leak(now, leakRatePerMs);
      }
      allowed[0] = existing.tryAdd(limitPerWindow);
      return existing;
    });
    return allowed[0];
  }

  private static class Hits {

    private long lastLeakTimestamp;
    private long requests;

    public Hits(long lastLeakTimestamp, int requests) {
      this.lastLeakTimestamp = lastLeakTimestamp;
      this.requests = requests;
    }

    void leak(long now, long leakRatePerMs) {
      long elapsedTimeInMs = now - lastLeakTimestamp;
      lastLeakTimestamp = now;
      long leaked = elapsedTimeInMs * leakRatePerMs;
      requests = Math.max(0, requests - leaked);
    }

    boolean tryAdd(long limitPerWindow) {
      if (limitPerWindow - requests >= SCALE) {
        requests += SCALE;
        return true;
      }
      return false;
    }
  }
}
