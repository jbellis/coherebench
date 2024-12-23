package io.github.jbellis;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import io.github.jbellis.BuildIndex.DataIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

import static io.github.jbellis.BuildIndex.INITIAL_BATCH_SIZE;
import static io.github.jbellis.BuildIndex.SKIP_COUNT;
import static io.github.jbellis.BuildIndex.convertToCql;
import static io.github.jbellis.BuildIndex.log;
import static io.github.jbellis.BuildIndex.printStats;

public class CassandraFlavor {
    private static final int CONCURRENT_WRITES = 100;
    private static final int CONCURRENT_READS = 16;
    private static CqlSession session;
    private static Semaphore semaphore;
    private static int totalRowsInserted;

    public static void benchmark() throws IOException, InterruptedException {
        // set up C* session
        var configBuilder = DriverConfigLoader.programmaticBuilder()
                // timeouts go to 11
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(600));

        session = CqlSession.builder()
                .withConfigLoader(configBuilder.build())
                .withKeyspace(CqlIdentifier.fromCql("coherebench"))
                .build();
        log("Connected to Cassandra.");

        var insertCql = "INSERT INTO embeddings_table (id, language, title, url, passage, embedding) VALUES (?, ?, ?, ?, ?, ?)";
        var insertStmt = session.prepare(insertCql);
        var simpleAnnCql = "SELECT id, title, url, passage FROM embeddings_table ORDER BY embedding ANN OF ? LIMIT 10";
        var simpleAnnStmt = session.prepare(simpleAnnCql);
        var restrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'sq' ORDER BY embedding ANN OF ? LIMIT 10";
        var restrictiveAnnStmt = session.prepare(restrictiveAnnCql);
        var unrestrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'en' ORDER BY embedding ANN OF ? LIMIT 10";
        var unrestrictiveAnnStmt = session.prepare(unrestrictiveAnnCql);

        try (var iterator = BuildIndex.dataSource()) {
            int batchSize = INITIAL_BATCH_SIZE;
            for (int i = 0; i < SKIP_COUNT; i++) {
                iterator.next();
                if (i % 100_000 == 0) {
                    log("Skipped %d rows", i);
                }
            }

            totalRowsInserted = SKIP_COUNT;
            while (totalRowsInserted < 10_000_000) {
                log("Batch size %d", batchSize);
                // Stats collectors
                var insertLatencies = new ArrayList<Long>();
                var simpleQueryLatencies = new ArrayList<Long>();
                var restrictiveQueryLatencies = new ArrayList<Long>();
                var unrestrictiveQueryLatencies = new ArrayList<Long>();

                // Progress tracking
                long batchStartTime = System.currentTimeMillis();
                long lastProgressTime = batchStartTime;
                AtomicInteger completedRequests = new AtomicInteger(0);
                
                // Start progress reporting thread
                int finalBatchSize = batchSize;
                Thread progressThread = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(5000); // Report every 5 seconds
                            long now = System.currentTimeMillis();
                            double elapsedSeconds = (now - batchStartTime) / 1000.0;
                            int completed = completedRequests.get();
                            double reqPerSec = completed / elapsedSeconds;
                            double remainingRequests = finalBatchSize - completed;
                            double estimatedSecondsRemaining = remainingRequests / reqPerSec;
                            
                            log("Progress: %d/%d requests (%.1f req/s), est. %.1f minutes remaining",
                                completedRequests.get(), finalBatchSize, reqPerSec,
                                estimatedSecondsRemaining / 60.0);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                });
                progressThread.start();

                // Insert rows
                semaphore = new Semaphore(CONCURRENT_WRITES);
                for (int i = 0; i < batchSize; i++) {
                    var rowData = iterator.next();
                    var language = ThreadLocalRandom.current().nextDouble() < 0.01 ? "sq" : "en";
                    var bound = insertStmt.bind(rowData._id(), language, rowData.title(), rowData.url(), rowData.text(), convertToCql(rowData.embedding()));
                    semaphore.acquire();
                    long start = System.nanoTime();
                    var asyncResult = session.executeAsync(bound);
                    asyncResult.whenComplete((rs, th) -> {
                        long latency = System.nanoTime() - start;
                        insertLatencies.add(latency);
                        if (th != null) {
                            log("Failed to insert row %s: %s", rowData._id(), th);
                        }
                        completedRequests.incrementAndGet();
                        semaphore.release();
                    });
                }
                while (semaphore.availablePermits() < CONCURRENT_WRITES) {
                    Thread.onSpinWait();
                }
                
                // Stop progress thread
                progressThread.interrupt();
                try {
                    progressThread.join();
                } catch (InterruptedException e) {
                    // ignore
                }
                
                printStats("Insert", insertLatencies);
                totalRowsInserted += batchSize;
                batchSize = totalRowsInserted; // double every time

                log("Waiting for compactions to finish...");
                waitForCompactionsToFinish();

                // Perform queries
                log("Performing queries");
                semaphore = new Semaphore(CONCURRENT_READS);
                executeQueriesAndCollectStats(simpleAnnStmt, iterator, simpleQueryLatencies);
                printStats("Simple Query", simpleQueryLatencies);
                executeQueriesAndCollectStats(restrictiveAnnStmt, iterator, restrictiveQueryLatencies);
                printStats("Restrictive Query", restrictiveQueryLatencies);
                executeQueriesAndCollectStats(unrestrictiveAnnStmt, iterator, unrestrictiveQueryLatencies);
                printStats("Unrestrictive Query", unrestrictiveQueryLatencies);
            }
        }
    }

    private static void executeQueriesAndCollectStats(PreparedStatement stmt, DataIterator iterator, List<Long> latencies) throws InterruptedException {
        // warmup with 10%
        for (int i = 0; i < 1_000; i++) {
            var rowData = iterator.next();
            var bound = stmt.bind(convertToCql(rowData.embedding()));
            semaphore.acquire();
            var asyncResult = session.executeAsync(bound);
            asyncResult.whenComplete((rs, th) -> {
                if (th != null) {
                    log("Failed to query row %s: %s", rowData._id(), th);
                }
                semaphore.release();
            });
        }
        while (semaphore.availablePermits() < CONCURRENT_READS) {
            Thread.onSpinWait();
        }

        // time the actual workload
        for (int i = 0; i < 10_000; i++) {
            var rowData = iterator.next();
            var bound = stmt.bind(convertToCql(rowData.embedding()));
            semaphore.acquire();
            long start = System.nanoTime();
            var asyncResult = session.executeAsync(bound);
            asyncResult.whenComplete((rs, th) -> {
                long latency = System.nanoTime() - start;
                latencies.add(latency);
                if (th != null) {
                    log("Failed to query row %s: %s", rowData._id(), th);
                }
                semaphore.release();
            });
        }
        while (semaphore.availablePermits() < CONCURRENT_READS) {
            Thread.onSpinWait();
        }
    }

    private static void waitForCompactionsToFinish() throws IOException, InterruptedException {
        // first flush
        String flushCmd = BuildIndex.config.getNodetoolPath() + " flush";
        Process flushProcess = Runtime.getRuntime().exec(flushCmd);
        flushProcess.waitFor();

        // then wait for compactions
        String statsCmd = BuildIndex.config.getNodetoolPath() + " compactionstats";
        outer:
        while (true) {
            Process process = Runtime.getRuntime().exec(statsCmd);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("pending tasks: 0")) {
                    break outer;
                }
            }
            process.waitFor();
            //noinspection BusyWait
            Thread.sleep(1000);
        }

        // snapshot
        String snapshotCmd = BuildIndex.config.getNodetoolPath() + " snapshot coherebench -cf embeddings_table -t cb-" + totalRowsInserted;
        Process snapshotProcess = Runtime.getRuntime().exec(snapshotCmd);
        snapshotProcess.waitFor();
    }
}
