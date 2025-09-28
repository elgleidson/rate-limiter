package com.github.elgleidson.ratelimiter;

public interface RateLimiter {

  boolean isAllowed(String identifier);

}
