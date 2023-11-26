package com.github.rlindooren.metrics.model;

public record LatencyMeasurement(long timestampMs, String system, String operation, double latencyMs) {
}
