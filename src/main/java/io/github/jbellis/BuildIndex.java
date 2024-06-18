package io.github.jbellis;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.JsonStringArrayList;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

public class BuildIndex {
    private static final Config config = new Config();
    private static final int N_SHARDS = 378;
    private static CqlSession session;

    public static void main(String[] args) throws IOException {
        // set up C* session
        config.validateDatasetPath();
        session = CqlSession.builder()
                .withKeyspace(CqlIdentifier.fromCql("coherebench"))
                .build();
        log("Connected to Cassandra.");

        benchmark();
    }


    private static void benchmark() throws IOException {
        var insertCql = "INSERT INTO embeddings_table (id, language, title, url, passage, embedding) VALUES (?, ?, ?, ?, ?, ?)";
        var insertStmt = session.prepare(insertCql);
        var simpleAnnCql = "SELECT id, title, url, passage FROM embeddings_table ORDER BY embedding ANN OF ? LIMIT 10";
        var simpleAnnStmt = session.prepare(simpleAnnCql);
        var restrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'sq' ORDER BY embedding ANN OF ? LIMIT 10";
        var restrictiveAnnStmt = session.prepare(restrictiveAnnCql);
        var unrestrictiveAnnCql = "SELECT id, title, url, passage FROM embeddings_table WHERE language = 'en' ORDER BY embedding ANN OF ? LIMIT 10";
        var unrestrictiveAnnStmt = session.prepare(unrestrictiveAnnCql);

        // Stats collectors
        var insertLatencies = new ArrayList<Long>();
        var simpleQueryLatencies = new ArrayList<Long>();
        var restrictiveQueryLatencies = new ArrayList<Long>();
        var unrestrictiveQueryLatencies = new ArrayList<Long>();

        int totalRowsInserted = 0;
        try (RowIterator iterator = new RowIterator(0, N_SHARDS)) {
//            int batchSize = 1 << 17; // 128k
            int batchSize = 1 << 10;

            while (totalRowsInserted < 10_000_000) {
                // Insert rows
                var insertFutures = ConcurrentHashMap.<CompletionStage<?>>newKeySet();
                for (int i = 0; i < batchSize; i++) {
                    var rowData = iterator.next();
                    var bound = insertStmt.bind(rowData._id(), "en", rowData.title(), rowData.url(), rowData.text(), rowData.embedding());
                    long start = System.nanoTime();
                    var asyncResult = session.executeAsync(bound);
                    insertFutures.add(asyncResult);
                    asyncResult.whenComplete((rs, th) -> {
                        long latency = System.nanoTime() - start;
                        insertLatencies.add(latency);
                        if (th != null) {
                            log("Failed to insert row %s: %s", rowData._id(), th);
                        }
                        insertFutures.remove(asyncResult);
                    });
                }
                log("Waiting for inserts to complete");
                while (!insertFutures.isEmpty()) {
                    Thread.onSpinWait();
                }
                totalRowsInserted += batchSize;

                // Perform queries
                log("Performing queries");
                executeQueriesAndCollectStats(simpleAnnStmt, iterator, simpleQueryLatencies);
                executeQueriesAndCollectStats(restrictiveAnnStmt, iterator, restrictiveQueryLatencies);
                executeQueriesAndCollectStats(unrestrictiveAnnStmt, iterator, unrestrictiveQueryLatencies);
            }

            // Print the stats
            printStats("Insert", insertLatencies);
            printStats("Simple Query", simpleQueryLatencies);
            printStats("Restrictive Query", restrictiveQueryLatencies);
            printStats("Unrestrictive Query", unrestrictiveQueryLatencies);
        }
    }

    private static void executeQueriesAndCollectStats(PreparedStatement stmt, RowIterator iterator, List<Long> latencies) {
        var futures = ConcurrentHashMap.<CompletionStage<?>>newKeySet();
        for (int i = 0; i < 10_000; i++) {
            var rowData = iterator.next();
            var bound = stmt.bind((Object) rowData.embedding());
            long start = System.nanoTime();
            var asyncResult = session.executeAsync(bound);
            futures.add(asyncResult);
            asyncResult.whenComplete((rs, th) -> {
                long latency = System.nanoTime() - start;
                latencies.add(latency);
                if (th != null) {
                    log("Failed to query row %s: %s", rowData._id(), th);
                }
                futures.remove(asyncResult);
            });
        }
        while (!futures.isEmpty()) {
            Thread.onSpinWait();
        }
    }

    private static void printStats(String operationType, List<Long> latencies) {
        if (latencies.isEmpty()) {
            System.out.println(operationType + " - No data collected.");
            return;
        }
        long sum = 0;
        for (long latency : latencies) {
            sum += latency;
        }
        double averageLatency = (sum / (double) latencies.size()) / 1_000_000;

        Collections.sort(latencies);
        long p50 = latencies.get((int) (latencies.size() * 0.50)) / 1_000_000;
        long p90 = latencies.get((int) (latencies.size() * 0.90)) / 1_000_000;
        long p99 = latencies.get((int) (latencies.size() * 0.99)) / 1_000_000;

        System.out.println(operationType + " Statistics:");
        System.out.println("    Average latency (ms): " + averageLatency);
        System.out.println("    50th percentile latency (ms): " + p50);
        System.out.println("    90th percentile latency (ms): " + p90);
        System.out.println("    99th percentile latency (ms): " + p99);
    }

    private static class RowIterator implements Iterator<RowData>, Closeable {
        private final RootAllocator allocator = new RootAllocator();
        private FileInputStream fileInputStream;
        private ArrowStreamReader reader;
        private final int endShardIndex;
        private boolean hasNextBatch;
        private VectorSchemaRoot root;

        private int nextShardIndex;
        private int currentRowIndex = 0;

        RowIterator(int startShardIndex, int endShardIndex) throws IOException {
            this.endShardIndex = endShardIndex;
            initReader(startShardIndex);
            nextShardIndex = startShardIndex + 1;
        }

        private void initReader(int shardIndex) throws IOException {
            if (fileInputStream != null) {
                fileInputStream.close(); // Close previous fileInputStream if exists
            }
            fileInputStream = new FileInputStream(config.filenameForShard(shardIndex));
            reader = new ArrowStreamReader(fileInputStream, allocator);
            hasNextBatch = reader.loadNextBatch();
            root = reader.getVectorSchemaRoot();
        }

        @Override
        public boolean hasNext() {
            return currentRowIndex < root.getRowCount() || hasNextBatch || nextShardIndex <= endShardIndex;
        }

        @Override
        public RowData next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available.");
            }
            if (currentRowIndex >= root.getRowCount()) {
                loadNextAvailableBatch();
            }
            return createRowData(currentRowIndex++);
        }

        private void loadNextAvailableBatch() {
            try {
                while (true) {
                    if (reader.loadNextBatch()) {
                        currentRowIndex = 0;
                        return;
                    }
                    if (nextShardIndex > endShardIndex) {
                        throw new NoSuchElementException("No more batches available.");
                    }
                    initReader(nextShardIndex++);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private RowData createRowData(int rowIndex) {
            String id = root.getVector("_id").getObject(rowIndex).toString();
            String url = root.getVector("url").getObject(rowIndex).toString();
            String title = root.getVector("title").getObject(rowIndex).toString();
            String passage = root.getVector("text").getObject(rowIndex).toString();
            var jsonList = (JsonStringArrayList<?>) root.getVector("emb").getObject(rowIndex);
            var embedding = convertToVector(jsonList);
            return new RowData(id, url, title, passage, embedding);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            fileInputStream.close();
            allocator.close();
        }
    }

    private static List<Float> convertToVector(JsonStringArrayList<?> jsonList) {
        var floatArray = new Float[jsonList.size()];
        for (int i = 0; i < jsonList.size(); i++) {
            floatArray[i] = Float.parseFloat(jsonList.get(i).toString());
        }
        return Arrays.asList(floatArray);
    }

    private static void log(String message, Object... args) {
        var timestamp = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        System.out.format(timestamp + ": " + message + "%n", args);
    }
}
