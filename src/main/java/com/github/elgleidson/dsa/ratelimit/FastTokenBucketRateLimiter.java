package com.github.elgleidson.dsa.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class FastTokenBucketRateLimiter implements RateLimiter {

  // 100 requests / min => 100 / 60_000 ms
  // Using floating points that will give us:
  // - limitPerWindow = 100
  // - refillRatePerMs = 0.00166667 tokens / ms
  //
  // After 100ms that gives us 0.166667 token.
  // After 600ms that gives us 1 whole token.
  //
  // Using long scale to avoid floating point arithmetic that will give us:
  // - limitPerWindow = 100_000_000 micro-tokens
  // - refillRatePerMs = 1666 micro-tokens / ms, same as the first 6 decimal places of our floating point impl = 0.001666 * scale
  //
  // After 100ms that gives us 166_600 micro-tokens
  // After 600ms that gives us 999_600 micro-tokens (a little less than 1 whole token)
  // After 601ms that gives us 1_001_266 micro-tokens (a little more than 1 whole token)
  //
  // It's a trade-off:
  // - Floating point is more precise, but more costly, and we can have rounding issues.
  // - Long scale is faster, no rounding issues, but it's not as precise. Increasing scale could lead to math overflow.
  private static final int SCALE = 1_000_000;

  private final long limitPerWindow; // in micro-tokens
  private final long refillRatePerMs; // in micro-tokens
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public FastTokenBucketRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  FastTokenBucketRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.limitPerWindow = limitPerWindow * SCALE;
    this.refillRatePerMs = this.limitPerWindow / windowSizeInMs;
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

    Hits(long lastRefillTimestamp, long tokens) {
      this.lastRefillTimestamp = lastRefillTimestamp;
      this.tokens = tokens;
    }

    void refill(long now, long limitPerWindow, long refillRatePerMs) {
      long elapsedTimeInMs = now - lastRefillTimestamp;
      lastRefillTimestamp = now;
      long newTokens = elapsedTimeInMs * refillRatePerMs;
      tokens = Math.min(limitPerWindow, tokens + newTokens);
    }

    boolean tryConsume() {
      if (tokens >= SCALE) {
        tokens -= SCALE;
        return true;
      }
      return false;
    }
  }
}
