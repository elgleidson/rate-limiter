package com.github.elgleidson.dsa.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class LeakyBucketRateLimiter implements RateLimiter {

  private final int limitPerWindow;
  private final double leakRatePerMs;
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public LeakyBucketRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  LeakyBucketRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.limitPerWindow = limitPerWindow;
    this.leakRatePerMs = (double) limitPerWindow / windowSizeInMs;
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
    private int requests;
    private double remainder;

    public Hits(long lastLeakTimestamp, int requests) {
      this.lastLeakTimestamp = lastLeakTimestamp;
      this.requests = requests;
      this.remainder = 0;
    }

    void leak(long now, double leakRatePerMs) {
      long elapsedTimeInMs = now - lastLeakTimestamp;
      lastLeakTimestamp = now;
      double leaked = remainder + (elapsedTimeInMs * leakRatePerMs);
      int wholeRequests = (int) leaked;
      if (wholeRequests > 0) {
        requests = Math.max(0, requests - wholeRequests);
        remainder = leaked - wholeRequests;
      } else {
        remainder = leaked;
      }
    }

    boolean tryAdd(int limitPerWindow) {
      if (requests < limitPerWindow) {
        requests++;
        return true;
      }
      return false;
    }
  }
}
