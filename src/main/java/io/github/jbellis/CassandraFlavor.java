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

import static io.github.jbellis.BuildIndex.convertToCql;
import static io.github.jbellis.BuildIndex.log;
import static io.github.jbellis.BuildIndex.printStats;

public class CassandraFlavor implements AutoCloseable {
    private static final int CONCURRENT_WRITES = 100;

    private final int CONCURRENT_READS = 16;
    private final CqlSession session;
    private Semaphore semaphore;
    private final AtomicInteger totalRowsInserted = new AtomicInteger(0);
    private final PreparedStatement insertStmt;
    private final PreparedStatement simpleAnnStmt;
    private final PreparedStatement restrictiveAnnStmt;
    private final PreparedStatement unrestrictiveAnnStmt;

    public CassandraFlavor() {
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
        this.insertStmt = session.prepare(insertCql);
        var simpleAnnCql = "SELECT id, title, url, passage FROM embeddings_table ORDER BY embedding ANN OF ? LIMIT 10";
        this.simpleAnnStmt = session.prepare(simpleAnnCql);
        var restrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'sq' ORDER BY embedding ANN OF ? LIMIT 10";
        this.restrictiveAnnStmt = session.prepare(restrictiveAnnCql);
        var unrestrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'en' ORDER BY embedding ANN OF ? LIMIT 10";
        this.unrestrictiveAnnStmt = session.prepare(unrestrictiveAnnCql);
    }

    private void executeQueriesAndCollectStats(PreparedStatement stmt, DataIterator iterator, List<Long> latencies) throws InterruptedException {
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

    private void waitForCompactionsToFinish() throws IOException, InterruptedException {
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

    public void insert(int numRows, int skipRows) throws IOException, InterruptedException {
        semaphore = new Semaphore(CONCURRENT_WRITES);
        try (DataIterator dataIterator = BuildIndex.dataSource()) {
            // Skip rows if requested
            for (int i = 0; i < skipRows; i++) {
                dataIterator.next();
            }

            // Insert the requested number of rows
            for (int i = 0; i < numRows; i++) {
                var rowData = dataIterator.next();
                var bound = insertStmt.bind(
                        rowData._id(),
                        "en",
                        rowData.title(),
                        rowData.url(),
                        rowData.text(),
                        convertToCql(rowData.embedding()));

                semaphore.acquire();
                var asyncResult = session.executeAsync(bound);
                asyncResult.whenComplete((rs, th) -> {
                    if (th != null) {
                        log("Failed to insert row %s: %s", rowData._id(), th);
                    }
                    totalRowsInserted.incrementAndGet();
                    semaphore.release();
                });

                if (i > 0 && i % 100_000 == 0) {
                    log("Inserted %d rows", i);
                }
            }
        }

        // Wait for all inserts to complete
        while (semaphore.availablePermits() < CONCURRENT_WRITES) {
            Thread.onSpinWait();
        }

        waitForCompactionsToFinish();
    }

    public void querySimple() throws IOException, InterruptedException {
        semaphore = new Semaphore(CONCURRENT_READS);
        var latencies = new ArrayList<Long>();
        try (var iterator = BuildIndex.dataSource()) {
            executeQueriesAndCollectStats(simpleAnnStmt, iterator, latencies);
        }
        printStats("Simple ANN Query", latencies);
    }

    public void queryRestrictive() throws IOException, InterruptedException {
        semaphore = new Semaphore(CONCURRENT_READS);
        var latencies = new ArrayList<Long>();
        try (var iterator = BuildIndex.dataSource()) {
            executeQueriesAndCollectStats(restrictiveAnnStmt, iterator, latencies);
        }
        printStats("Restrictive ANN Query", latencies);
    }

    public void queryUnrestrictive() throws IOException, InterruptedException {
        semaphore = new Semaphore(CONCURRENT_READS);
        var latencies = new ArrayList<Long>();
        try (var iterator = BuildIndex.dataSource()) {
            executeQueriesAndCollectStats(unrestrictiveAnnStmt, iterator, latencies);
        }
        printStats("Unrestrictive ANN Query", latencies);
    }

    @Override
    public void close() {
        session.close();
    }
}
