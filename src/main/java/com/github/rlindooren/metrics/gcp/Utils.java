package com.github.rlindooren.metrics.gcp;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class Utils {

    public static <T> Thread createContiniouslyRunningThread(final String name,
                                                             final Callable<T> actionsToPerform,
                                                             final long waitTimeBetweenIterationsMs,
                                                             final Supplier<Boolean> shouldContinue,
                                                             final Logger log) {
        return new Thread(() -> {
            log.debug("Started thread: {}", name);
            while (shouldContinue.get()) {
                try {
                    actionsToPerform.call();
                    if (waitTimeBetweenIterationsMs > 0) {
                        Thread.sleep(waitTimeBetweenIterationsMs);
                    }
                } catch (InterruptedException e) {
                    if (shouldContinue.get()) {
                        log.warn("Interrupted while run = {}", shouldContinue.get(), e);
                    } else {
                        log.debug("Interrupted while run = {}", shouldContinue.get(), e);
                        break;
                    }
                } catch (Exception ex) {
                    log.error("Error in thread: {}", name, ex);
                }
            }
        }, name);
    }

    public static long now() {
        return System.currentTimeMillis();
    }

    private static final Map<String, Lock> locksForKeys = new HashMap<>();
    public static void doOnlyAfterAcquiringLock(final String key, final Runnable runnable, final Logger log) {
        Objects.requireNonNull(key);
        final var lock = locksForKeys.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        log.trace("Acquired lock for key: {}", key);
        try {
            runnable.run();
        } finally {
            lock.unlock();
            log.trace("Released lock for key: {}", key);
        }
    }
}
