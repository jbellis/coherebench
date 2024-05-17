package io.github.jbellis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Config {
    private final Map<String, Path> paths = new HashMap<>();
    private final String cassandraHost;

    public Config() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load configuration properties.", e);
        }
        paths.put("en", Path.of(props.getProperty("dataset_location")).resolve("Cohere___wikipedia-2023-11-embed-multilingual-v3/en/0.0.0/37feace541fadccf70579e9f289c3cf8e8b186d7/wikipedia-2023-11-embed-multilingual-v3-train-%s-of-00378.arrow"));
        cassandraHost = props.getProperty("CassandraHost");
    }

    public void validateDatasetPath() {
        var samplePath = Path.of(filenameForShard(0));
        if (!Files.exists(samplePath)) {
            System.out.format("Dataset does not exist at %s%nThis probably means you need to run download.py first", samplePath);
            System.exit(1);
        }
    }

    private String filenameForShard(String language, int shardIndex) {
        return String.format(paths.get(language).toString(), String.format("%05d", shardIndex));
    }

    public String filenameForShard(int shardIndex) {
        return filenameForShard("en", shardIndex);
    }

    public InetSocketAddress getCassandraHost() {
        // Extract the host and port from the config
        String[] hostParts = cassandraHost.split(":");

        String host = hostParts[0];
        int port = 9042; // Default Cassandra port
        if (hostParts.length > 1) {
            try {
                port = Integer.parseInt(hostParts[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number provided, using default: " + port);
            }
        }

        // Create InetSocketAddress with the extracted host and port
        return new InetSocketAddress(host, port);
    }
}
