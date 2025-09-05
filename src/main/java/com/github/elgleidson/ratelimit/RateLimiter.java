package com.github.elgleidson.ratelimit;

public interface RateLimiter {

  boolean isAllowed(String identifier);

}
