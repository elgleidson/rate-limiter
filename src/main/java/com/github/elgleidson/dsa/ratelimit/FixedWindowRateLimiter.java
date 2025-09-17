package com.github.elgleidson.dsa.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class FixedWindowRateLimiter implements RateLimiter {

  private final long windowSizeInMs;
  private final int limitPerWindow;
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public FixedWindowRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  FixedWindowRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.windowSizeInMs = windowSizeInMs;
    this.limitPerWindow = limitPerWindow;
    this.requests = new ConcurrentHashMap<>();
    this.currentTimeInMsSupplier = currentTimeInMsSupplier;
  }

  @Override
  public boolean isAllowed(String identifier) {
    var currentTimeWindow = timeWindow();
    var hits = requests.compute(identifier, (key, existing) -> {
      // Hits is mutable on purpose to avoid object allocation / CG every time.
      // So we only create Hits once, and then we keep updating it accordingly.
      // It doesn't stress the CG, so it's more performant.
      // No issue as compute() is atomic / thread-safe per key.

      if (existing == null) {
        // First request ever
        existing = new Hits(currentTimeWindow, 0);
      } else if (existing.timeWindow < currentTimeWindow) {
        // New time window → reset
        existing.timeWindow = currentTimeWindow;
        existing.requests = 0;
      }
      existing.requests++;
      return existing;
    });
    return hits.requests <= limitPerWindow;
  }

  private int timeWindow() {
    var now = currentTimeInMsSupplier.getAsLong();
    // Calculate the "current window number" based on the timestamp.
    // windowSizeInMs defines the length of a single window (e.g., 60_000 ms = 1 minute)
    //
    // Window number concept:
    // ---------------------
    // Each window spans windowSizeInMs milliseconds.
    //    windowNumber = (milliseconds since epoch) / (window size)
    // Requests in the same window share the same window number.
    //
    // Examples for 1-minute windows (windowSizeInMs = 60_000):
    //
    // Time (UTC)              | Epoch (ms)        | Window number
    // -----------------------------------------------------------
    // 2025-09-01 10:11:12.000 | 1_756_721_472_000 | 29278691
    // 2025-09-01 10:11:12.123 | 1_756_721_472_123 | 29278691
    // 2025-09-01 10:11:13.456 | 1_756_721_473_456 | 29278691
    // 2025-09-01 10:12:12.000 | 1_756_721_532_000 | 29278692 # next minute
    // 2025-09-02 10:12:12.000 | 1_756_807_932_000 | 29280132 # same time, but other day
    //
    // Diagram showing multiple IPs hitting within windows:
    //
    //   Time -> 10:11:12      10:11:45      10:12:05      10:12:50
    // IP1     [29278691,1]  [29278691,6]   [29278692,1]  [29278692,9]
    // IP2     [29278691,1]  [29278691,11]  [29278692,1]  [29278692,13]
    //
    // Each box represents a time window. Requests within the same box increment
    // the same counter; when window changes, the counter resets.
    //
    // This allows us to store only the window number (int = 4 bytes) per IP instead of a full timestamp (long = 8 bytes),
    // saving memory while still tracking which window each IP’s counter belongs to.
    return (int) (now / windowSizeInMs);
  }

  private static class Hits {

    private int timeWindow;
    private int requests;

    Hits(int timeWindow, int requests) {
      this.timeWindow = timeWindow;
      this.requests = requests;
    }
  }
}
