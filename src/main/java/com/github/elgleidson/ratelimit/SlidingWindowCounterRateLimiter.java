package com.github.elgleidson.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class SlidingWindowCounterRateLimiter implements RateLimiter {

  private final long windowSizeInMs;
  private final int limitPerWindow;
  private final Map<String, Hits> requests;
  private final LongSupplier currentTimeInMsSupplier;

  public SlidingWindowCounterRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  SlidingWindowCounterRateLimiter(long windowSizeInMs, int limitPerWindow,LongSupplier currentTimeInMsSupplier) {
    this.windowSizeInMs = windowSizeInMs;
    this.limitPerWindow = limitPerWindow;
    this.requests = new ConcurrentHashMap<>();
    this.currentTimeInMsSupplier = currentTimeInMsSupplier;
  }

  @Override
  public boolean isAllowed(String identifier) {
    var now = currentTimeInMsSupplier.getAsLong();
    var currentTimeWindow = timeWindow(now);
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
      // It could be simplified moving the calculations to the end (with some tweaks inside the ifs)
      // But that would mean floating point math would run in every single request, increasing CPU cost.
      // So let's keep it inside the ifs so these operations are done only when they are needed (same or next time window)

      if (existing == null) {
        // First request ever
        existing = new Hits(currentTimeWindow, 0, 1);
        allowed[0] = true;
      } else if (existing.timeWindow == currentTimeWindow) {
        // Same time window
        var overlap = overlap(now, currentTimeWindow);
        var projected = (existing.previousRequests * overlap) + existing.currentRequests + 1;
        allowed[0] = projected <= limitPerWindow;
        if (allowed[0]) {
          existing.currentRequests++;
        }
      } else if (existing.timeWindow + 1 == currentTimeWindow) {
        // One time window shift
        existing.timeWindow = currentTimeWindow;
        existing.previousRequests = existing.currentRequests;
        var overlap = overlap(now, currentTimeWindow);
        var projected = (existing.previousRequests * overlap) + 1;
        allowed[0] = projected <= limitPerWindow;
        existing.currentRequests = allowed[0] ? 1 : 0;
      } else {
        // More than one time window shift -> reset everything
        existing.timeWindow = currentTimeWindow;
        existing.previousRequests = 0;
        existing.currentRequests = 0;
        allowed[0] = true;
      }
      return existing;
    });
    return allowed[0];
  }

  private double overlap(long now, long currentWindow) {
    return 1.0 - ((double) (now - (currentWindow * windowSizeInMs))) / windowSizeInMs;
  }

  private int timeWindow(long now) {
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
    private int previousRequests;
    private int currentRequests;

    public Hits(int timeWindow, int previousRequests, int currentRequests) {
      this.timeWindow = timeWindow;
      this.previousRequests = previousRequests;
      this.currentRequests = currentRequests;
    }
  }
}
