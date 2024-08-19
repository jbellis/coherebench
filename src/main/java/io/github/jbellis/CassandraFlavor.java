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
import java.util.concurrent.ThreadLocalRandom;

import static io.github.jbellis.BuildIndex.INITIAL_BATCH_SIZE;
import static io.github.jbellis.BuildIndex.convertToCql;
import static io.github.jbellis.BuildIndex.log;
import static io.github.jbellis.BuildIndex.printStats;

public class CassandraFlavor {
    private static final int CONCURRENT_WRITES = 100;
    private static final int CONCURRENT_READS = 16;
    private static CqlSession session;
    private static Semaphore semaphore;

    public static void load() throws IOException, InterruptedException {
        connect();

        var insertCql = "INSERT INTO embeddings_table (id, b1, b2, b3, b4, b5, title, url, passage, embedding) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        var insertStmt = session.prepare(insertCql);

        try (var iterator = BuildIndex.dataSource()) {
            // Stats collectors
            var insertLatencies = new ArrayList<Long>();

            // Insert rows
            semaphore = new Semaphore(CONCURRENT_WRITES);
            for (int i = 0; i < INITIAL_BATCH_SIZE; i++) {
                var rowData = iterator.next();
                boolean b1 = ThreadLocalRandom.current().nextDouble() < 0.01;
                boolean b2 = ThreadLocalRandom.current().nextDouble() < 0.02;
                boolean b3 = ThreadLocalRandom.current().nextDouble() < 0.03;
                boolean b4 = ThreadLocalRandom.current().nextDouble() < 0.04;
                boolean b5 = ThreadLocalRandom.current().nextDouble() < 0.05;
                var bound = insertStmt.bind(rowData._id(), b1, b2, b3, b4, b5,
                                            rowData.title(), rowData.url(), rowData.text(), convertToCql(rowData.embedding()));
                semaphore.acquire();
                long start = System.nanoTime();
                var asyncResult = session.executeAsync(bound);
                asyncResult.whenComplete((rs, th) -> {
                    long latency = System.nanoTime() - start;
                    insertLatencies.add(latency);
                    if (th != null) {
                        log("Failed to insert row %s: %s", rowData._id(), th);
                    }
                    semaphore.release();
                });
            }
            while (semaphore.availablePermits() < CONCURRENT_WRITES) {
                Thread.onSpinWait();
            }
        }
    }

    public static void benchmark() {
        connect();
        semaphore = new Semaphore(CONCURRENT_READS);

        for (int i = 1; i <= 5; i++) {
            var cql = String.format("SELECT id, title, url, passage FROM embeddings_table WHERE b%d = true ORDER BY embedding ANN OF ? LIMIT 10", i);
            var stmt = session.prepare(cql);
            var latencies = new ArrayList<Long>();
            executeQueriesAndCollectStats(stmt, iterator, latencies);
            printStats(String.format("0.0%d selectivity", i), latencies);
        }
    }

    private static void connect() {
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
    }

    private static void executeQueriesAndCollectStats(PreparedStatement stmt, DataIterator iterator, List<Long> latencies) throws InterruptedException {
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
        String command = BuildIndex.config.getNodetoolPath() + " compactionstats";
        while (true) {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("pending tasks: 0")) {
                    return;
                }
            }
            process.waitFor();
            Thread.sleep(1000);
        }
    }
}
