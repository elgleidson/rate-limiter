package com.github.elgleidson.ratelimiter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeakyBucketRateLimiterTest {

  private RateLimiter rateLimiter;
  private AtomicLong fakeTime;

  @BeforeEach
  void setup() {
    // simulates System.currentTimeMillis() to keeps the tests simpler
    fakeTime = new AtomicLong(0);
  }

  private void advanceTime(long milliseconds) {
    fakeTime.addAndGet(milliseconds);
  }

  @Test
  void limitRespected() {
    rateLimiter = new LeakyBucketRateLimiter(60_000, 3, fakeTime::get);

    // 3 requests in the same time window -> allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();

    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();
  }

  @Test
  void leak() {
    rateLimiter = new LeakyBucketRateLimiter(60_000, 3, fakeTime::get);

    // 3 requests in the same time window -> allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 1
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 2
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 3
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();

    advanceTime(20_000); // 1/3 of the time has passed, so 1 new request is allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests 3 -> 2 -> 3
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();

    advanceTime(40_000); // 2/3 of the time has passed, another 2 requests are allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests 3 -> 1 -> 2
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests 2 -> 3
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();
  }

  @Test
  void leak_fractionalLeftover() {
    rateLimiter = new LeakyBucketRateLimiter(60_000, 3, fakeTime::get);

    // 3 requests in the same time window -> allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 1
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 2
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // requests -> 3
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();

    advanceTime(10_000); // 1/6 of the time has passed, so "0.5" request is allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse(); // requests 3 -> "2.5"
    advanceTime(10_000); // 1/6 of the time has passed, so another "0.5" request is allowed, completing 1 whole request
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue(); // token "2.5" -> 2 -> 3
  }

  @Test
  void separateIdentifiersAreIndependent() {
    rateLimiter = new LeakyBucketRateLimiter(60_000, 3, fakeTime::get);

    // 3 requests in the same time window -> allowed
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isTrue();
    Assertions.assertThat(rateLimiter.isAllowed("1.1.1.1")).isFalse();

    Assertions.assertThat(rateLimiter.isAllowed("2.2.2.2")).isTrue();
  }

  @Test
  void concurrentAccess_multipleIps_shouldRespectLimitPerWindow() throws InterruptedException {
    // Multiple threads issue requests to different identifiers (IP/client-id).
    // Ensures each identifier respects its own limit.
    rateLimiter = new LeakyBucketRateLimiter(100, 10, fakeTime::get);

    int threadCount = 50;
    int requestsPerThread = 20;
    List<String> ips = List.of("1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4", "5.5.5.5");

    Map<String, AtomicInteger> allowedCounts = new ConcurrentHashMap<>();
    try (var executor = Executors.newFixedThreadPool(threadCount)) {

      for (int t = 0; t < threadCount; t++) {
        String ip = ips.get(t % ips.size()); // rotate IPs
        executor.submit(() -> {
          for (int i = 0; i < requestsPerThread; i++) {
            var allowed = rateLimiter.isAllowed(ip);
            //System.out.println("ip: " + ip + ", allowed: " + allowed);
            if (allowed) {
              allowedCounts.computeIfAbsent(ip, k -> new AtomicInteger()).incrementAndGet();
            }
          }
        });
      }
      executor.shutdown();
      var finished = executor.awaitTermination(30, TimeUnit.SECONDS);
      if (!finished) {
        throw new RuntimeException("Threads did not finish in time");
      }
    }

    // Assertions
    for (String ip : ips) {
      var count = allowedCounts.getOrDefault(ip, new AtomicInteger(0));
      assertThat(count).as("IP " + ip + " exceeded the limit!").hasValueLessThanOrEqualTo(10);
    }
  }

  @Test
  void highContention_sameIp_shouldNotExceedLimit() throws InterruptedException {
    // Multiple threads hammer the same identifier (IP/client-id).
    // Ensures that even under high contention, the limit is never exceeded.
    rateLimiter = new LeakyBucketRateLimiter(100, 10, fakeTime::get);

    int threadCount = 50;
    int requestsPerThread = 20;
    String ip = "1.1.1.1";

    AtomicInteger allowedCount = new AtomicInteger();
    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (int t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          for (int i = 0; i < requestsPerThread; i++) {
            var allowed = rateLimiter.isAllowed(ip);
            //System.out.println("ip: " + ip + ", allowed: " + allowed);
            if (allowed) {
              allowedCount.incrementAndGet();
            }
          }
        });
      }
      executor.shutdown();
      var finished = executor.awaitTermination(30, TimeUnit.SECONDS);
      if (!finished) {
        throw new RuntimeException("Threads did not finish in time");
      }
    }

    assertThat(allowedCount).as("Limit was exceeded under contention!").hasValueLessThanOrEqualTo(10);
  }
}
