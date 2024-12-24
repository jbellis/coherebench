package io.github.jbellis;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.datastax.oss.driver.api.core.data.CqlVector;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class CohereBench {
    static final Config config = new Config();
    static final int N_SHARDS = 378;

    /**
     * Usage:
     * CB_CMD=insert CB_ROWS=1000000 CB_SKIP=500000 mvn compile exec:exec@run
     * CB_CMD=query CB_QUERY_TYPE=restrictive mvn compile exec:exec@run
     */
    public static void main(String[] args) throws Exception {
        var loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        var rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);

        try (CassandraFlavor flavor = new CassandraFlavor()) {
            String command = System.getenv("CB_CMD");
            if (command == null) {
                System.out.println("Usage: CB_CMD=<insert|query> [options] mvn compile exec:exec@run");
                return;
            }

            switch (command) {
                case "insert" -> {
                    int numRows = Integer.parseInt(System.getenv().getOrDefault("CB_INSERT_ROWS", "10000000"));
                    int skipRows = Integer.parseInt(System.getenv().getOrDefault("CB_SKIP", "0"));
                    flavor.insert(numRows, skipRows);
                }
                case "query" -> {
                    String type = System.getenv().getOrDefault("CB_QUERY_TYPE", "simple");
                    switch (type) {
                        case "simple" -> flavor.querySimple();
                        case "restrictive" -> flavor.queryRestrictive();
                        case "unrestrictive" -> flavor.queryUnrestrictive();
                        default -> System.out.println("Unknown query type: " + type);
                    }
                }
                default -> System.out.println("Unknown command: " + command);
            }
        }
    }

    static void printStats(String operationType, List<Long> rawLatencies) {
        // not sure why this is necessary
        var latencies = rawLatencies.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (latencies.isEmpty()) {
            return;
        }
        long sum = 0;
        for (var latency : latencies) {
            sum += latency;
        }
        double averageLatency = (sum / (double) latencies.size()) / 1_000_000;

        Collections.sort(latencies);
        double p50 = latencies.get((int) (latencies.size() * 0.50)) / 1_000_000.0;
        double p90 = latencies.get((int) (latencies.size() * 0.90)) / 1_000_000.0;
        double p99 = latencies.get((int) (latencies.size() * 0.99)) / 1_000_000.0;

        System.out.println(operationType + " Statistics:");
        System.out.printf("    Average latency (ms): %.2f%n", averageLatency);
        System.out.printf("    50th percentile latency (ms): %.2f%n", p50);
        System.out.printf("    90th percentile latency (ms): %.2f%n", p90);
        System.out.printf("    99th percentile latency (ms): %.2f%n", p99);
    }

    static DataIterator dataSource() throws IOException {
        return new RowIterator(0, N_SHARDS);
//        return new MockDataIterator();
    }

    private static class MockDataIterator implements DataIterator {
        @Override
        public void close() {
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public RowData next() {
            var uuid = java.util.UUID.randomUUID();
            var jsonList = new JsonStringArrayList<Float>(1024);
            for (int i = 0; i < 1024; i++) {
                jsonList.add(ThreadLocalRandom.current().nextFloat());
            }
            return new RowData(uuid.toString(), "http://example.com/" + uuid, "Title", "Text", jsonList);
        }
    }

    public interface DataIterator extends Iterator<RowData>, Closeable {}

    static class RowIterator implements DataIterator {
        private final RootAllocator allocator = new RootAllocator();
        private FileInputStream fileInputStream;
        private ArrowStreamReader reader;
        private final int endShardIndex;
        private boolean hasNextBatch;
        private VectorSchemaRoot root;

        private int nextShardIndex;
        private int currentRowIndex = 0;

        RowIterator(int startShardIndex, int endShardIndex) throws IOException {
            config.validate();
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
            return new RowData(id, url, title, passage, jsonList);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            fileInputStream.close();
            allocator.close();
        }
    }

    static CqlVector<Float> convertToCql(JsonStringArrayList<?> jsonList) {
        var floatArray = new Float[jsonList.size()];
        for (int i = 0; i < jsonList.size(); i++) {
            floatArray[i] = Float.parseFloat(jsonList.get(i).toString());
        }
        return CqlVector.newInstance(floatArray);
    }

    static float[] convertToArray(JsonStringArrayList<?> jsonList) {
        var floatArray = new float[jsonList.size()];
        for (int i = 0; i < jsonList.size(); i++) {
            floatArray[i] = Float.parseFloat(jsonList.get(i).toString());
        }
        return floatArray;
    }

    static void log(String message, Object... args) {
        var timestamp = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        System.out.format(timestamp + ": " + message + "%n", args);
    }

    record RowData(String _id, String url, String title, String text, JsonStringArrayList<?> embedding) {}
}
