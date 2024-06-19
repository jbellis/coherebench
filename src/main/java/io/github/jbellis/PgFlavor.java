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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.github.jbellis.BuildIndex.convertToArray;
import static io.github.jbellis.BuildIndex.log;
import static io.github.jbellis.BuildIndex.printStats;

public class PgFlavor {
    private static final int CONCURRENT_REQUESTS = 100;
    private static ExecutorService executorService;

    // ThreadLocal for Connections
    private static final ThreadLocal<Connection> connection = ThreadLocal.withInitial(() -> {
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/coherebench", "postgres", "postgres");
            PGvector.addVectorType(conn);
            return conn;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    });

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

    public static void benchmark() throws IOException, InterruptedException, SQLException {
        executorService = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);

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
                    var start = System.nanoTime();
                    executorService.submit(() -> {
                        try {
                            PreparedStatement stmt = insertStmt.get();
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
                executorService.shutdown();
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                executorService = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
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
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            if (connection.get() != null) {
                connection.get().close();
            }
        }
    }

    private static void executeQueriesAndCollectStats(ThreadLocal<PreparedStatement> stmt, RowIterator iterator, List<Long> latencies) throws InterruptedException {
        for (int i = 0; i < 10_000; i++) {
            var rowData = iterator.next();
            var start = System.nanoTime();
            executorService.submit(() -> {
                try {
                    PreparedStatement statement = stmt.get();
                    statement.setObject(1, convertToArray(rowData.embedding()));
                    statement.executeQuery();
                    long latency = System.nanoTime() - start;
                    latencies.add(latency);
                } catch (SQLException e) {
                    log("Failed to query row %s: %s", rowData._id(), e);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        executorService = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
    }
}



