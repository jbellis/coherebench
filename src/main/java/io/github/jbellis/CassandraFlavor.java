package io.github.jbellis;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import io.github.jbellis.BuildIndex.DataIterator;
import io.github.jbellis.BuildIndex.RowIterator;

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
    private static final int CONCURRENT_REQUESTS = 100;
    private static CqlSession session;
    private static Semaphore semaphore;

    public static void benchmark() throws IOException, InterruptedException {
        // set up C* session
        var configBuilder = DriverConfigLoader.programmaticBuilder()
                // timeouts go to 11
                .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, java.time.Duration.ofSeconds(600))
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(600));
        // no way to do this in the driver, apparently
        semaphore = new Semaphore(CONCURRENT_REQUESTS);

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

        int totalRowsInserted = 0;
        try (var iterator = BuildIndex.dataSource()) {
//            int batchSize = 1 << 17; // 128k
            int batchSize = INITIAL_BATCH_SIZE;

            while (totalRowsInserted < 10_000_000) {
                log("Batch size %d", batchSize);
                // Stats collectors
                var insertLatencies = new ArrayList<Long>();
                var simpleQueryLatencies = new ArrayList<Long>();
                var restrictiveQueryLatencies = new ArrayList<Long>();
                var unrestrictiveQueryLatencies = new ArrayList<Long>();

                // Insert rows
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
                        semaphore.release();
                    });
                }
                while (semaphore.availablePermits() < CONCURRENT_REQUESTS) {
                    Thread.onSpinWait();
                }
                totalRowsInserted += batchSize;
                batchSize = totalRowsInserted; // double every time

                log("Waiting for compactions to finish...");
                var start = System.currentTimeMillis();
                waitForCompactionsToFinish();
                log("Compactions took %d seconds", (System.currentTimeMillis() - start) / 1000);

                // Perform queries
                log("Performing queries");
                executeQueriesAndCollectStats(simpleAnnStmt, iterator, simpleQueryLatencies);
                executeQueriesAndCollectStats(restrictiveAnnStmt, iterator, restrictiveQueryLatencies);
                executeQueriesAndCollectStats(unrestrictiveAnnStmt, iterator, unrestrictiveQueryLatencies);

                // Print the stats
                printStats("Insert", insertLatencies);
                printStats("Simple Query", simpleQueryLatencies);
                printStats("Restrictive Query", restrictiveQueryLatencies);
                printStats("Unrestrictive Query", unrestrictiveQueryLatencies);
            }
        }
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
        while (semaphore.availablePermits() < CONCURRENT_REQUESTS) {
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
