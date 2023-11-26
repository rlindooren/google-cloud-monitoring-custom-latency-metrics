package com.github.rlindooren.metrics;

import com.github.rlindooren.metrics.gcp.GoogleCloudMetricsServiceImpl;
import com.github.rlindooren.metrics.model.ApplicationIdentifier;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Emulates a dummy application that registers latency measurements after calling external services.
 * Which are then published to the GCP Metrics API.
 */
public class PlainJavaExample {

    public static boolean run = true;

    public static void main(String[] args) throws Exception {
        // ‼️update this when trying out this example
        final String gcpProjectId;
        if (System.getenv().containsKey("GCP_PROJECT_ID")) {
            gcpProjectId = System.getenv("GCP_PROJECT_ID");
        } else {
            gcpProjectId = "your-project";
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> run = false));
        final var dummyApplication = new DummyApplication(gcpProjectId);
        dummyApplication.run();
    }

    public static class DummyApplication {

        final MetricsService metricsService;
        final Random random = new Random();

        public DummyApplication(final String gcpProjectId) throws IOException {
            final var applicationName = "dummyApplication";
            // In case of running in K8S then the container id could be a suitable candidate for this value
            final var instanceId = UUID.randomUUID().toString();

            final var metricServiceClient = MetricServiceClient.create();
            metricsService = new GoogleCloudMetricsServiceImpl(
                    gcpProjectId,
                    metricServiceClient,
                    new ApplicationIdentifier(applicationName, instanceId)
            );
        }

        /**
         * Emulates doing work that requires a call to two external services.
         * The latency/duration of those calls are then registered as custom metric.
         */
        public void doSomething() {
            final var stopWatch = Stopwatch.createStarted();
            emulateCallToExternalService();
            stopWatch.stop();
            metricsService.registerLatencyMeasurement("orderService", "createOrder", stopWatch.elapsed(TimeUnit.MILLISECONDS));

            stopWatch.reset().start();
            emulateCallToExternalService();
            stopWatch.stop();
            metricsService.registerLatencyMeasurement("paymentService", "createPayment", stopWatch.elapsed(TimeUnit.MILLISECONDS));
        }

        /**
         * Starts some 'clients' that do 'something' to emulate a call to two external services.
         * These measurements are then registered for sending to the GCP Metrics API.
         */
        public void run() {
            final var nrOfClients = 4;
            createDummyClients(nrOfClients).forEach(Thread::start);
        }

        public void emulateCallToExternalService() {
            // Emulate wait time (latency) for this 'service' to complete
            try {
                var latencyMs = random.nextLong(1, 500);
                // Simulate a very long wait time in 1% of the cases
                if (random.nextInt(100) == 0) {
                    latencyMs += 5_000;
                }
                Thread.sleep(latencyMs);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        private List<DummyClientThread> createDummyClients(int nrOfClients) {
            return IntStream.range(1, nrOfClients + 1)
                    .mapToObj(this::createDummyClient)
                    .collect(Collectors.toList());
        }

        private DummyClientThread createDummyClient(int clientNr) {
            return new DummyClientThread(clientNr, () -> {
                System.out.println("Started thread: " + Thread.currentThread().getName());
                while (run) {
                    doSomething();
                    try {
                        // Wait a bit before making the next set of calls again
                        Thread.sleep(random.nextLong(1, 100));
                    } catch (InterruptedException e) {
                        if (run) {
                            throw new RuntimeException(e);
                        }
                        break;
                    }
                }
            });
        }

        private static class DummyClientThread extends Thread {
            public DummyClientThread(int nr, Runnable work) {
                super(work, "DummyClient-" + nr);
            }
        }
    }
}
