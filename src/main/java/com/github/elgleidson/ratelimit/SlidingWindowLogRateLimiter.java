package com.github.elgleidson.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public class SlidingWindowLogRateLimiter implements RateLimiter {

  private final long windowSizeInMs;
  private final int limitPerWindow;
  private final LongSupplier currentTimeInMsSupplier;
  private final Map<String, Hits> requests;

  public  SlidingWindowLogRateLimiter(long windowSizeInMs, int limitPerWindow) {
    this(windowSizeInMs, limitPerWindow, System::currentTimeMillis);
  }

  SlidingWindowLogRateLimiter(long windowSizeInMs, int limitPerWindow, LongSupplier currentTimeInMsSupplier) {
    this.windowSizeInMs = windowSizeInMs;
    this.limitPerWindow = limitPerWindow;
    this.currentTimeInMsSupplier = currentTimeInMsSupplier;
    this.requests = new ConcurrentHashMap<>();
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
      if (existing == null) {
        // First request ever
        existing = new Hits(limitPerWindow);
      } else {
        // Remove timestamps outside the time window
        while (!existing.isEmpty() && existing.peek() <= (now - windowSizeInMs)) {
          existing.poll();
        }
      }
      allowed[0] = existing.tryAdd(now);
      return existing;
    });
    return allowed[0];
  }

  private static class Hits {

    private final long[] timestamps;
    private int head = 0;
    private int tail = 0;
    private int size = 0;

    private Hits(int capacity) {
      this.timestamps = new long[capacity];
    }

    boolean isEmpty() {
      return size == 0;
    }

    void add(long timestamp) {
      timestamps[tail] = timestamp;
      tail = (tail + 1) % timestamps.length;
      size++;
    }

    boolean tryAdd(long timestamp) {
      if (size < timestamps.length) {
        add(timestamp);
        return true;
      } else {
        return false;
      }
    }

    long peek() {
      return timestamps[head];
    }

    long poll() {
      long timestamp = timestamps[head];
      timestamps[head] = -1; // to clear the value
      head = (head + 1) % timestamps.length;
      size--;
      return timestamp;
    }
  }
}
