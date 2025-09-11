package com.github.elgleidson.dsa.ratelimit;

public interface RateLimiter {

  boolean isAllowed(String identifier);

}
