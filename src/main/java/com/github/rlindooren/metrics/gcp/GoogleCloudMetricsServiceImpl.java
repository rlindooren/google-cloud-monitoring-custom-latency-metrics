package com.github.rlindooren.metrics.gcp;

import com.github.rlindooren.metrics.MetricsService;
import com.github.rlindooren.metrics.model.ApplicationIdentifier;
import com.github.rlindooren.metrics.model.LatencyDistribution;
import com.github.rlindooren.metrics.model.LatencyMeasurement;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.collect.EvictingQueue;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.rlindooren.metrics.gcp.Utils.*;
import static org.slf4j.LoggerFactory.getLogger;

public class GoogleCloudMetricsServiceImpl implements MetricsService, Closeable {

    private final static Logger log = getLogger(GoogleCloudMetricsServiceImpl.class);

    /**
     * The incoming latency measurements are stored in this queue.
     * It is a 'ring buffer' with a maximum size. Evicting the oldest measurement when the queue is full.
     */
    private final EvictingQueue<LatencyMeasurement> latencyMeasurementsBuffer;

    /**
     * The closed latency distributions are stored in this queue.
     * These are the distributions that are ready to be sent to the Google Cloud Monitoring API.
     * It is a 'ring buffer' with a maximum size. Evicting the oldest measurement when the queue is full.
     */
    private final EvictingQueue<LatencyDistribution> closedLatencyDistributionsBuffer;

    /**
     * The interval in seconds in which a distribution is closed and then send to the GCP Monitoring API.
     * The distribution window is kept open for this amount of time.
     */
    private final int distributionWindowSeconds;

    /**
     * The bounds for the buckets in the latency distribution.
     */
    private final List<Double> latencyDistributionBounds;

    /**
     * The latency distributions that are currently open.
     * One per system/operation combination.
     */
    private final Map<String, LatencyDistribution> activeLatencyDistributions;

    private final MetricServiceClient metricServiceClient;

    private final GcpMetricsAPIUtils gcpMetricsAPIUtils;

    private final String gcpProjectId;

    private final ApplicationIdentifier applicationIdentifier;

    private final String metricNameForLatencyMeasurements;

    private boolean run;

    private static Thread measurementsDistributorThread;
    private static Thread distributionsCloserThread;
    private static Thread closedDistributionsSenderThread;

    private long droppedMeasurementsSequence;

    private final ExecutorService executorService;

    /**
     * Instantiate with default settings.
     */
    public GoogleCloudMetricsServiceImpl(@Nonnull final String gcpProjectId,
                                         @Nonnull final MetricServiceClient metricServiceClient,
                                         @Nonnull final ApplicationIdentifier applicationIdentifier) {
        this(
                gcpProjectId,
                metricServiceClient, LatencyDistribution.DEFAULT_LATENCY_DISTRIBUTION_BOUNDS,
                applicationIdentifier,
                Defaults.DEFAULT_METRIC_NAME_FOR_LATENCY,
                Defaults.DEFAULT_DISTRIBUTION_WINDOW_SECONDS,
                Defaults.DEFAULT_MAX_RETAINED_MEASUREMENTS_IN_MEMORY,
                new GcpMetricsAPIUtils(),
                Executors.newCachedThreadPool()
        );
    }

    /**
     * Allows to instantiate with custom settings.
     */
    public GoogleCloudMetricsServiceImpl(@Nonnull final String gcpProjectId,
                                         @Nonnull final MetricServiceClient metricServiceClient,
                                         @Nonnull final List<Double> latencyDistributionBounds,
                                         @Nonnull final ApplicationIdentifier applicationIdentifier,
                                         @Nonnull final String metricNameForLatencyMeasurements,
                                         final int distributionWindowSeconds,
                                         final int maxRetainedMeasurements,
                                         final GcpMetricsAPIUtils gcpMetricsAPIUtils,
                                         final ExecutorService executorService) {
        this.gcpProjectId = Objects.requireNonNull(gcpProjectId);
        this.latencyDistributionBounds = Objects.requireNonNull(latencyDistributionBounds);
        this.applicationIdentifier = Objects.requireNonNull(applicationIdentifier);
        this.metricNameForLatencyMeasurements = Objects.requireNonNull(metricNameForLatencyMeasurements);
        this.distributionWindowSeconds = distributionWindowSeconds;
        this.metricServiceClient = Objects.requireNonNull(metricServiceClient);
        this.gcpMetricsAPIUtils = Objects.requireNonNull(gcpMetricsAPIUtils);
        this.executorService = Objects.requireNonNull(executorService);
        latencyMeasurementsBuffer = EvictingQueue.create(maxRetainedMeasurements);
        // TODO: different max size for closed distributions buffer?
        closedLatencyDistributionsBuffer = EvictingQueue.create(maxRetainedMeasurements);
        activeLatencyDistributions = new HashMap<>();
        droppedMeasurementsSequence = 0;
        log.debug("Instantiated with settings: {}", this);
        initializeAndStart();
    }

    protected synchronized void initializeAndStart() {
        log.trace("Initializing and starting");
        run = true;

        // Start a thread that continuously consumes incoming latency measurements and places them in their distributions
        if (measurementsDistributorThread == null) {
            measurementsDistributorThread = createContiniouslyRunningThread("MeasurementsDistributor", () -> {
                for (LatencyMeasurement measurement = latencyMeasurementsBuffer.poll(); measurement != null; measurement = latencyMeasurementsBuffer.poll()) {
                    processLatencyMeasurement(measurement);
                }
                return null;
            }, 200, () -> run, log);
            measurementsDistributorThread.start();
        }

        // Start a thread that closes all distributions and places them in the queue of closed distributions to be send to GCP.
        if (distributionsCloserThread == null) {
            distributionsCloserThread = createContiniouslyRunningThread("DistributionsCloser", () -> {
                closeDistributions();
                return null;
            }, 1000L * distributionWindowSeconds, () -> run, log);
            distributionsCloserThread.start();
        }

        // Start a thread that processes the closed distributions and sends them to GCP.
        if (closedDistributionsSenderThread == null) {
            // TODO: make this wait time configurable
            closedDistributionsSenderThread = createContiniouslyRunningThread("ClosedDistributionsSender", () -> {
                processClosedDistributions();
                return null;
            }, 1000L, () -> run, log);
            closedDistributionsSenderThread.start();
        }
    }

    @Override
    public void close() {
        log.debug("Closing");
        run = false;
        distributionsCloserThread.interrupt();
        measurementsDistributorThread.interrupt();
        closedDistributionsSenderThread.interrupt();
        closeDistributions();
        processClosedDistributions();
    }

    @Override
    public boolean registerLatencyMeasurement(@Nonnull final String system,
                                              @Nonnull String operation,
                                              final long latencyMs) {
        return registerLatencyMeasurement(now(), system, operation, latencyMs);
    }

    protected boolean registerLatencyMeasurement(final long timestamp,
                                                 final String system,
                                                 final String operation,
                                                 final long latencyMs) {
        final var measurement = new LatencyMeasurement(timestamp, system, operation, latencyMs);
        if (latencyMeasurementsBuffer.remainingCapacity() <= 0) {
            droppedMeasurementsSequence++;
            // Don't log every dropped measurement to prevent flooding the logs
            if (droppedMeasurementsSequence == 1 || droppedMeasurementsSequence % 1000 == 0) {
                log.warn("Latency measurements buffer is full. The oldest measurement will be dropped. Dropped so far: {}", droppedMeasurementsSequence);
            }
            return false;
        }
        latencyMeasurementsBuffer.offer(measurement);
        droppedMeasurementsSequence = 0;
        log.trace("Added latency measurement to queue: {}", measurement);
        return true;
    }

    /**
     * Using a simple locking strategy to prevent concurrent modification of the latency distributions.
     */
    protected void processLatencyMeasurement(final LatencyMeasurement measurement) {
        log.trace("Processing latency measurement: {}", measurement);
        final var keyForDistribution = createKeyForDistribution(measurement.system(), measurement.operation());
        doOnlyAfterAcquiringLock(keyForDistribution, () -> {
            final var distribution = getActiveDistribution(measurement.system(), measurement.operation());
            if (distribution.addLatency(measurement.latencyMs())) {
                log.trace("Added latency measurement to distribution: {}", distribution);
            } else {
                log.warn("Failed to add latency measurement to distribution: {}", distribution);
            }
        }, log);
    }

    /**
     * Closes all active distributions and places them in the queue of closed distributions to be send to GCP.
     * Using a simple locking strategy to prevent concurrent modification of the latency distributions.
     */
    protected void closeDistributions() {
        log.trace("Closing distributions");
        final var keysOfAllActiveDistributions = new HashSet<>(activeLatencyDistributions.keySet());
        for (final String key : keysOfAllActiveDistributions) {
            doOnlyAfterAcquiringLock(key, () -> {
                final var distribution = activeLatencyDistributions.remove(key);
                if (distribution == null) {
                    log.warn("Distribution for key {} is null. This should not happen.", key);
                    return;
                }
                distribution.close();
                log.trace("Closed distribution: {}", distribution);
                if (distribution.getCount() > 0) {
                    log.trace("Adding distribution to queue for sending to GCP: {}", distribution);
                    closedLatencyDistributionsBuffer.add(distribution);
                } else {
                    log.trace("Distribution is empty. Not adding to closed distributions queue: {}", distribution);
                }
            }, log);
        }
    }

    protected synchronized void processClosedDistributions() {
        log.trace("Processing closed distributions.");
        if (closedLatencyDistributionsBuffer.isEmpty()) {
            log.trace("No closed distributions to process at this moment.");
            return;
        }
        final var batchForSending = new ArrayList<LatencyDistribution>();
        for (var distribution = closedLatencyDistributionsBuffer.poll(); distribution != null; distribution = closedLatencyDistributionsBuffer.poll()) {
            batchForSending.add(distribution);
        }
        sendClosedDistributions(batchForSending, applicationIdentifier);
    }

    protected void sendClosedDistributions(final List<LatencyDistribution> distributions,
                                           final ApplicationIdentifier applicationIdentifier) {
        final var request = gcpMetricsAPIUtils.createTimeSeriesRequests(
                distributions,
                gcpProjectId,
                metricNameForLatencyMeasurements,
                gcpMetricsAPIUtils.createGenericMetricLabels(applicationIdentifier),
                true
        );
        request.forEach(this::sendRequest);
    }

    /**
     * Asynchronous and using a poor man's retry mechanism.
     */
    private void sendRequest(final CreateTimeSeriesRequest request) {
        executorService.execute(() -> {
            // TODO: make this configurable
            int maxAttempts = 3;
            int attempts = 0;
            int wait_between_attempts_ms = 1000;
            while (attempts <= maxAttempts) {
                try {
                    attempts++;
                    log.trace("Sending request. attempt={}/{}", attempts, maxAttempts);
                    metricServiceClient.createTimeSeries(request);
                    log.trace("Sent request:\n{}", request);
                    break;
                } catch (Exception ex) {
                    final var retryAble = retryableException(ex);
                    if (!retryAble || attempts > maxAttempts) {
                        log.warn("Error sending request (retryAble: {}, attempt: {}/{}):\n{}",
                                retryAble, attempts, maxAttempts, request, ex);
                        break;
                    }
                    log.warn("Error sending request (retryAble: {}, attempt: {}/{}):\n{}",
                            retryAble, attempts, maxAttempts, request, ex);
                    try {
                        Thread.sleep((long) wait_between_attempts_ms * attempts);
                    } catch (InterruptedException e) {
                        log.debug("Interrupted!");
                    }
                }
            }
        });
    }

    protected boolean retryableException(final Exception ex) {
        // E.g.: com.google.api.gax.rpc.InternalException: io.grpc.StatusRuntimeException: INTERNAL: One or more TimeSeries could not be written: Internal error encountered. Please retry after a few seconds. If internal errors persist, contact support at https://cloud.google.com/support/docs.: global{} timeSeries[0,1]: custom.googleapis.com/generic_latency{instance:13dbdb95-1e1f-4b94-b379-3a1a81d7811d,application:dummyApplication,operation:createOrder,service:orderService}
        return ex.getMessage().toLowerCase().contains("please retry");
        // TODO: add more checks
    }

    protected LatencyDistribution getActiveDistribution(final String system, final String operation) {
        final var keyForDistribution = createKeyForDistribution(system, operation);
        return activeLatencyDistributions.computeIfAbsent(keyForDistribution, key ->
                new LatencyDistribution(system, operation, latencyDistributionBounds, now())
        );
    }

    protected String createKeyForDistribution(final String system, final String operation) {
        return String.format("%s/%s", system, operation);
    }

    @Override
    public String toString() {
        return "GoogleCloudMetricsServiceImpl{" +
                "distributionWindowSeconds=" + distributionWindowSeconds +
                ", latencyDistributionBounds=" + latencyDistributionBounds +
                ", gcpProjectId='" + gcpProjectId + '\'' +
                ", applicationIdentifier=" + applicationIdentifier +
                ", metricNameForLatencyMeasurements='" + metricNameForLatencyMeasurements + '\'' +
                '}';
    }
}
