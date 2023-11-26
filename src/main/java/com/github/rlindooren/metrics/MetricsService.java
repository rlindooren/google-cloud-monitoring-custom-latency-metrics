package com.github.rlindooren.metrics;

import javax.annotation.Nonnull;

public interface MetricsService {

    /**
     * Register a latency measurement, using `now` as timestamp.
     *
     * @param system    The system that was measured (e.g. "order-service", "redis", etc...). Will be used as label.
     * @param operation The operation that was measured (e.g. "createOrder", "get", etc...). Will be used as label.
     * @param latencyMs The latency in milliseconds. How long it took to get a response from the given operation of the given system.
     *
     * @return true if the measurement was successfully registered for further processing, false otherwise.
     */
    boolean registerLatencyMeasurement(@Nonnull final String system,
                                       @Nonnull final String operation,
                                       final long latencyMs);

}
