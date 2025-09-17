package com.github.elgleidson.dsa.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class TokenBucketRateLimiter implements RateLimiter {

  private final int limitPerWindow;
  private final double refillRatePerMs;
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public TokenBucketRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  TokenBucketRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.limitPerWindow = limitPerWindow;
    this.refillRatePerMs = (double) limitPerWindow / windowSizeInMs;
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
        existing = new Hits(now, limitPerWindow);
      } else {
        existing.refill(now, limitPerWindow, refillRatePerMs);
      }
      allowed[0] = existing.tryConsume();
      return existing;
    });
    return allowed[0];
  }

  private static class Hits {

    private long lastRefillTimestamp;
    private long tokens;
    private double remainder;

    public Hits(long lastRefillTimestamp, int tokens) {
      this.lastRefillTimestamp = lastRefillTimestamp;
      this.tokens = tokens;
      this.remainder = 0;
    }

    void refill(long now, int limitPerWindow, double refillRatePerMs) {
      long elapsedTimeInMs = now - lastRefillTimestamp;
      lastRefillTimestamp = now;
      double newTokens = remainder + (elapsedTimeInMs * refillRatePerMs);
      int wholeTokens = (int) newTokens;
      if (wholeTokens > 0) {
        tokens = Math.min(limitPerWindow, tokens + wholeTokens);
        remainder = newTokens % wholeTokens;
      } else {
        remainder = newTokens;
      }
    }

    boolean tryConsume() {
      if (tokens > 0) {
        tokens--;
        return true;
      }
      return false;
    }
  }
}
