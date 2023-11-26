package com.github.rlindooren.metrics.gcp;

import com.github.rlindooren.metrics.model.ApplicationIdentifier;
import com.github.rlindooren.metrics.model.LatencyDistribution;
import com.google.api.Distribution;
import com.google.api.Metric;
import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.monitoring.v3.*;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class GcpMetricsAPIUtils {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(GcpMetricsAPIUtils.class);

    // The maximum list size is 200 and each object in the list must specify a different time series
    // https://cloud.google.com/monitoring/custom-metrics/creating-metrics#writing-ts
    public static final int MAX_TIMESERIES_PER_REQUEST = 200;

    /**
     * Empty distributions will be ignored.
     * Requests will be split up, each containing maximum 200 time-series per request <a href="https://cloud.google.com/monitoring/custom-metrics/creating-metrics#writing-ts">source</a>.
     */
    public List<CreateTimeSeriesRequest> createTimeSeriesRequests(
            @Nonnull final List<LatencyDistribution> distributions,
            @Nonnull final String gcpProjectId,
            @Nonnull final String metricName,
            @Nonnull final Map<String, String> genericLabelsForMetric,
            final boolean includeAdditionalTsForCount) {

        Objects.requireNonNull(distributions);

        if (distributions.isEmpty()) {
            log.warn("No distributions to create time series request for.");
            return Collections.emptyList();
        }

        Objects.requireNonNull(gcpProjectId);
        Objects.requireNonNull(metricName);
        Objects.requireNonNull(genericLabelsForMetric);

        final var orderedDistributions = new ArrayList<>(distributions);
        orderedDistributions.sort(Comparator.comparingLong(LatencyDistribution::getFromTimestampMs));

        final var tsForDistributions = orderedDistributions.stream()
                .map(distribution -> createTimeSeriesForLatencyDistribution(
                        distribution,
                        gcpProjectId,
                        metricName,
                        genericLabelsForMetric
                )).toList();
        final var allTimeSeries = new ArrayList<>(tsForDistributions);
        if (includeAdditionalTsForCount) {
            final var tsForCounts = orderedDistributions.stream()
                    .map(distribution -> createTimeSeriesForLatencyDistributionCount(
                            distribution,
                            gcpProjectId,
                            metricName,
                            genericLabelsForMetric
                    )).toList();
            allTimeSeries.addAll(tsForCounts);
        }

        final var requests = new ArrayList<CreateTimeSeriesRequest>();
        var requestBuilder = createRequestBuilder(gcpProjectId);

        for (TimeSeries timeSeries : allTimeSeries) {
            if (requestBuilder.getTimeSeriesCount() < MAX_TIMESERIES_PER_REQUEST) {
                requestBuilder.addTimeSeries(timeSeries);
            } else {
                log.trace("Reached maximum number of time series per request: {}", MAX_TIMESERIES_PER_REQUEST);
                requests.add(requestBuilder.build());
                requestBuilder = createRequestBuilder(gcpProjectId);
            }
        }

        if (requestBuilder.getTimeSeriesCount() > 0) {
            requests.add(requestBuilder.build());
        }

        log.debug("Created {} request(s) from {} timeSeries created for {} distribution(s)",
                requests.size(), allTimeSeries.size(), distributions.size());

        return requests;
    }

    private static CreateTimeSeriesRequest.Builder createRequestBuilder(String gcpProjectId) {
        return CreateTimeSeriesRequest.newBuilder()
                .setName(ProjectName.of(gcpProjectId).toString());
    }

    public TimeSeries createTimeSeriesForLatencyDistribution(
            @Nonnull final LatencyDistribution latencyDistribution,
            @Nonnull final String gcpProjectId,
            @Nonnull final String metricName,
            @Nonnull final Map<String, String> genericLabelsForMetric) {

        final var value = TypedValue.newBuilder()
                .setDistributionValue(createDistribution(latencyDistribution))
                .build();

        final var interval = TimeInterval.newBuilder()
                .setStartTime(Timestamps.fromMillis(latencyDistribution.getFromTimestampMs()))
                .setEndTime(Timestamps.fromMillis(latencyDistribution.getTillTimestampMs()))
                .build();

        final var fullMetricName = getFullMetricName(metricName);

        return createTimeSeries(
                latencyDistribution,
                gcpProjectId,
                fullMetricName,
                genericLabelsForMetric,
                value,
                interval,
                MetricDescriptor.MetricKind.CUMULATIVE,
                MetricDescriptor.ValueType.DISTRIBUTION,
                "ms"
        );
    }

    public TimeSeries createTimeSeriesForLatencyDistributionCount(
            @Nonnull final LatencyDistribution latencyDistribution,
            @Nonnull final String gcpProjectId,
            @Nonnull final String metricName,
            @Nonnull final Map<String, String> genericLabelsForMetric) {

        final var value = TypedValue.newBuilder()
                .setInt64Value(latencyDistribution.getCount())
                .build();

        final var interval = TimeInterval.newBuilder()
                .setEndTime(Timestamps.fromMillis(latencyDistribution.getTillTimestampMs()))
                .build();

        // TODO: make configurable
        final var fullMetricName = getFullMetricName(metricName) + "/count";

        return createTimeSeries(
                latencyDistribution,
                gcpProjectId,
                fullMetricName,
                genericLabelsForMetric,
                value,
                interval,
                MetricDescriptor.MetricKind.GAUGE,
                MetricDescriptor.ValueType.INT64,
                null
        );
    }

    private TimeSeries createTimeSeries(
            final LatencyDistribution latencyDistribution,
            final String gcpProjectId,
            final String fullMetricName,
            final Map<String, String> genericLabelsForMetric,
            final TypedValue value,
            final TimeInterval interval,
            final MetricDescriptor.MetricKind metricKind,
            final MetricDescriptor.ValueType valueType,
            @Nullable final String unit) {

        Objects.requireNonNull(latencyDistribution);
        Objects.requireNonNull(gcpProjectId);
        Objects.requireNonNull(fullMetricName);
        Objects.requireNonNull(genericLabelsForMetric);
        Objects.requireNonNull(value);
        Objects.requireNonNull(interval);
        Objects.requireNonNull(metricKind);
        Objects.requireNonNull(valueType);

        // Prepares the metric descriptor
        final var allLabels = new HashMap<String, String>();
        allLabels.putAll(genericLabelsForMetric);
        allLabels.putAll(createDistributionSpecificLabels(latencyDistribution));
        final var metric = Metric.newBuilder()
                .setType(fullMetricName)
                .putAllLabels(allLabels)
                .build();

        final var monitoredResource = MonitoredResource.newBuilder()
                .setType("global")
                .putAllLabels(Map.of("project_id", gcpProjectId))
                .build();

        // Each TimeSeries object must contain only a single Point object
        // https://cloud.google.com/monitoring/custom-metrics/creating-metrics#writing-ts
        final var point = Point.newBuilder()
                .setInterval(interval)
                .setValue(value)
                .build();
        final var tsBuilder = TimeSeries.newBuilder()
                .setMetricKind(metricKind)
                .setValueType(valueType)
                .setMetric(metric)
                .setResource(monitoredResource)
                .addPoints(point);
        if (unit != null) {
            tsBuilder.setUnit(unit);
        }
        return tsBuilder.build();
    }

    private static Distribution createDistribution(LatencyDistribution latencyDistribution) {
        return Distribution.newBuilder()
                .setBucketOptions(Distribution.BucketOptions.newBuilder()
                        .setExplicitBuckets(
                                Distribution.BucketOptions.Explicit.newBuilder()
                                        .addAllBounds(latencyDistribution.getBoundsForBuckets())
                                        .build()
                        )
                        .build()
                )
                .setMean(latencyDistribution.getMean())
                .setCount(latencyDistribution.getCount())
                .addAllBucketCounts(latencyDistribution.getAllCountsForBuckets())
                .build();
    }

    protected String getFullMetricName(final String metricName) {
        return String.format("custom.googleapis.com/%s", metricName);
    }

    protected Map<String, String> createDistributionSpecificLabels(final LatencyDistribution distribution) {
        // TODO: make keys (labels) configurable
        return Map.of(
                "service", distribution.getService(),
                "operation", distribution.getOperation()
        );
    }

    public Map<String, String> createGenericMetricLabels(@Nonnull final ApplicationIdentifier applicationIdentifier) {
        // TODO: make keys (labels) configurable
        return Map.of(
                "application", applicationIdentifier.serviceName(),
                "instance", applicationIdentifier.instanceId()
        );
    }

    private String validateMetricName(final String metricName) throws IllegalArgumentException {
        // TODO - https://cloud.google.com/monitoring/api/v3/naming-conventions
        return metricName;
    }

    private Map<String, String> validateMetricLabels(final Map<String, String> metricLabels) throws IllegalArgumentException {
        metricLabels.forEach(this::validateMetricLabel);
        return metricLabels;
    }

    private String validateMetricLabel(final String metricLabel, final String metricLabelValue) throws IllegalArgumentException {
        // TODO
        // Use lower-case letters (a-z), digits (0-9), and underscores (_) in a label key.
        // You must start label keys with a letter.
        // The maximum length of a label key is 100 characters.
        // Each key must be unique within the metric type.
        // You can have no more than 30 labels per metric type.
        // https://cloud.google.com/monitoring/api/v3/naming-conventions
        return metricLabel;
    }
}
