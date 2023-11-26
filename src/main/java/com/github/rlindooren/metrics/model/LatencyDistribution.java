package com.github.rlindooren.metrics.model;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class LatencyDistribution {

    private static final Logger log = getLogger(LatencyDistribution.class);

    private final String service;
    private final String operation;
    private final List<Double> boundsForBuckets;
    private final long fromTimestampMs;
    private long tillTimestampMs;
    private boolean open;
    private final Map<Integer, AtomicInteger> countsForBuckets;
    private long count;
    private long latencySum;

    /**
     * Please note that the bounds are copied, sorted and then an additional bound is appended (for overflow).
     */
    public LatencyDistribution(@Nonnull final String service,
                               @Nonnull final String operation,
                               @Nonnull final List<Double> boundsForBuckets,
                               final long fromTimestampMs) {
        this.service = Objects.requireNonNull(service);
        this.operation = Objects.requireNonNull(operation);

        this.boundsForBuckets = new ArrayList<>(Objects.requireNonNull(boundsForBuckets));
        this.boundsForBuckets.sort(Double::compareTo);
        // Add bound for 'overflow'
        //this.boundsForBuckets.add(Double.MAX_VALUE); <-- too much for GCP, it seems
        // Using 60 minutes as the last bound
        this.boundsForBuckets.add((double) (1000 * 60 * 60));

        this.fromTimestampMs = fromTimestampMs;

        countsForBuckets = new HashMap<>(boundsForBuckets.size());

        open = true;
        count = 0;
        latencySum = 0;
    }

    public boolean addLatency(final double latencyMs) {
        if (!open) {
            log.warn("Distribution is closed. Latency measurement {}ms will be ignored.", latencyMs);
            return false;
        }
        final var bucketIndex = getBucketIndex(latencyMs, boundsForBuckets);
        if (bucketIndex < 0 || bucketIndex >= boundsForBuckets.size()) {
            log.warn("Latency measurement {}ms is outside of the bounds of the buckets: {}/{}. This measurement will be ignored.",
                    latencyMs, bucketIndex, boundsForBuckets.size() - 1);
            return false;
        }
        countsForBuckets.computeIfAbsent(bucketIndex, key -> new AtomicInteger()).incrementAndGet();
        latencySum += latencyMs;
        count++;
        return true;
    }

    public void close() {
        open = false;
        tillTimestampMs = System.currentTimeMillis();
    }

    protected int getBucketIndex(final double latencyMs, final List<Double> bucketBounds) {
        if (latencyMs < 0) {
            return 0;
        }
        for (int index = 0; index < bucketBounds.size(); index++) {
            if (latencyMs <= bucketBounds.get(index)) {
                return index;
            }
        }
        return bucketBounds.size() - 1;
    }

    public String getService() {
        return service;
    }

    public String getOperation() {
        return operation;
    }

    public long getFromTimestampMs() {
        return fromTimestampMs;
    }

    public long getTillTimestampMs() {
        return tillTimestampMs;
    }

    public void setTillTimestampMs(long tillTimestampMs) {
        this.tillTimestampMs = tillTimestampMs;
    }

    public List<Double> getBoundsForBuckets() {
        return new ArrayList<>(boundsForBuckets);
    }

    public Map<Integer, Integer> getCountsForBuckets() {
        return countsForBuckets.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().get()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * In the order of the boundsForBuckets.
     */
    public List<Long> getAllCountsForBuckets() {
        return boundsForBuckets.stream()
                .map(bound -> getBucketIndex(bound, boundsForBuckets))
                .map(countsForBuckets::get)
                .map(ai -> Optional.ofNullable(ai).map(AtomicInteger::get).orElse(0))
                .map(Long::valueOf)
                .collect(Collectors.toList());
    }

    /**
     * Count of all values in the distribution.
     */
    public long getCount() {
        return count;
    }

    /**
     * All values accumulated, divided by the count of values.
     */
    public double getMean() {
        if (count == 0) {
            return 0;
        }
        return (double) latencySum / count;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    // -------------------

    @Override
    public String toString() {
        return "LatencyDistribution{" +
                "service='" + service + '\'' +
                ", operation='" + operation + '\'' +
                ", open=" + open +
                ", fromTimestampMs=" + fromTimestampMs +
                ", tillTimestampMs=" + tillTimestampMs +
                ", boundsForBuckets=" + boundsForBuckets +
                ", countsForBuckets=" + countsForBuckets +
                ", allCountsForBuckets=" + getAllCountsForBuckets() +
                ", count=" + count +
                ", mean=" + getMean() +
                '}';
    }

    /**
     * Bucket distribution example: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]
     * <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#explicit-bucket-histogram-aggregation">source</a>
     */
    public final static List<Double> DEFAULT_LATENCY_DISTRIBUTION_BOUNDS = List.of(
            0.0,
            5.0,
            10.0,
            25.0,
            50.0,
            75.0,
            100.0,
            250.0,
            500.0,
            750.0,
            1000.0,
            2500.0,
            5000.0,
            7500.0,
            10000.0
    );
}
