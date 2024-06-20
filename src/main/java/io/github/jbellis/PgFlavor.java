package io.github.jbellis;

import com.pgvector.PGvector;
import io.github.jbellis.BuildIndex.RowIterator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.github.jbellis.BuildIndex.convertToArray;
import static io.github.jbellis.BuildIndex.log;
import static io.github.jbellis.BuildIndex.printStats;

public class PgFlavor {
    private static final int CONCURRENT_REQUESTS = 100;
    private static ExecutorService executorService;

    private static final Set<Connection> connections = ConcurrentHashMap.newKeySet();
    // ThreadLocal for Connections
    private static final ThreadLocal<Connection> connection = ThreadLocal.withInitial(() -> {
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/coherebench", "postgres", "postgres");
            PGvector.addVectorType(conn);
            connections.add(conn);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });
    private static void closeAll() {
        for (Connection conn : connections) {
            try {
                conn.close();
            } catch (SQLException e) {
                log("Failed to close connection: %s", e);
            }
        }
    }

    // ThreadLocal for PreparedStatements
    private static final ThreadLocal<PreparedStatement> insertStmt = ThreadLocal.withInitial(() -> {
        try {
            return connection.get().prepareStatement("INSERT INTO coherebench.embeddings_table (id, language, title, url, passage, embedding) VALUES (?, ?, ?, ?, ?, ?::vector)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<PreparedStatement> simpleAnnStmt = ThreadLocal.withInitial(() -> {
        try {
            return connection.get().prepareStatement("SELECT id, title, url, passage FROM coherebench.embeddings_table ORDER BY embedding <#> ?::vector LIMIT 10");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<PreparedStatement> restrictiveAnnStmt = ThreadLocal.withInitial(() -> {
        try {
            return connection.get().prepareStatement("SELECT id, title, url, passage FROM coherebench.embeddings_table WHERE language = 'sq' ORDER BY embedding <#> ?::vector LIMIT 10");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    private static final ThreadLocal<PreparedStatement> unrestrictiveAnnStmt = ThreadLocal.withInitial(() -> {
        try {
            return connection.get().prepareStatement("SELECT id, title, url, passage FROM coherebench.embeddings_table WHERE language = 'en' ORDER BY embedding <#> ?::vector LIMIT 10");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

    public static void benchmark() throws IOException, InterruptedException {
        executorService = createExecutor();

        int totalRowsInserted = 0;
        try (RowIterator iterator = new RowIterator(0, BuildIndex.N_SHARDS)) {
            int batchSize = 1 << 10;

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
                    executorService.submit(() -> {
                        try {
                            PreparedStatement stmt = insertStmt.get();
                            var start = System.nanoTime();
                            stmt.setString(1, rowData._id());
                            stmt.setString(2, language);
                            stmt.setString(3, rowData.title());
                            stmt.setString(4, rowData.url());
                            stmt.setString(5, rowData.text());
                            stmt.setObject(6, convertToArray(rowData.embedding()));
                            stmt.executeUpdate();
                            long latency = System.nanoTime() - start;
                            insertLatencies.add(latency);
                        } catch (SQLException e) {
                            log("Failed to insert row %s: %s", rowData._id(), e);
                        }
                    });
                }
                drainAndReinitExecutor();
                totalRowsInserted += batchSize;
                batchSize = totalRowsInserted; // double every time

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

    private static void drainAndReinitExecutor() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        closeAll();
        executorService = createExecutor();
    }

    private static ExecutorService createExecutor() {
        // Custom rejection policy
        return new ThreadPoolExecutor(
                CONCURRENT_REQUESTS,
                CONCURRENT_REQUESTS,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(2 * CONCURRENT_REQUESTS),
                (runnable, executor) -> {
                    try {
                        // Block the caller until there's space in the queue
                        executor.getQueue().put(runnable);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // Restore the interrupted status
                        throw new RejectedExecutionException("Interrupted while waiting for space in the queue", e);
                    }
                }
        );
    }

    private static void executeQueriesAndCollectStats(ThreadLocal<PreparedStatement> stmt, RowIterator iterator, List<Long> latencies) throws InterruptedException {
        for (int i = 0; i < 10_000; i++) {
            var rowData = iterator.next();
            executorService.submit(() -> {
                try {
                    PreparedStatement statement = stmt.get();
                    var start = System.nanoTime();
                    statement.setObject(1, convertToArray(rowData.embedding()));
                    statement.executeQuery();
                    long latency = System.nanoTime() - start;
                    latencies.add(latency);
                } catch (SQLException e) {
                    log("Failed to query row %s: %s", rowData._id(), e);
                }
            });
        }
        drainAndReinitExecutor();
    }
}



