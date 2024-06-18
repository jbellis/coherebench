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

    public Config() {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            props.load(fis);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load configuration properties.", e);
        }
        paths.put("en", Path.of(props.getProperty("dataset_location")).resolve("Cohere___wikipedia-2023-11-embed-multilingual-v3/en/0.0.0/37feace541fadccf70579e9f289c3cf8e8b186d7/wikipedia-2023-11-embed-multilingual-v3-train-%s-of-00378.arrow"));
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
}
