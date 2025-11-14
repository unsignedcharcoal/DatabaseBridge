package dev.charcoal.database.bridge.file.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import dev.charcoal.database.bridge.file.FileDatabase;
import dev.charcoal.database.bridge.file.exception.YamlDatabaseException;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class YamlDatabase<O> extends FileDatabase<O> {

    private final File baseFile;
    private final ObjectMapper mapper;
    private final Map<String, O> map;
    private final Class<O> type;
    private final String rootKey;

    @Getter
    private boolean loaded = false;

    /**
     * Creates a YAML database reader without a root section (flat structure).
     */
    public YamlDatabase(File file, Class<O> type) {
        this(file, type, null);
    }

    /**
     * Creates a YAML database reader that loads data from a root key (e.g. "ranks").
     */
    public YamlDatabase(File file, Class<O> type, @Nullable String rootKey) {
        this.baseFile = file;
        this.mapper = new ObjectMapper(new YAMLFactory());
        this.map = new ConcurrentHashMap<>();
        this.type = type;
        this.rootKey = rootKey;
    }

    @Override
    protected void refreshDatabase() {
        if (!baseFile.exists()) {
            try {
                baseFile.getParentFile().mkdirs();
                baseFile.createNewFile();
                System.out.println("Created new YAML file: " + baseFile.getAbsolutePath());
            } catch (IOException e) {
                throw new YamlDatabaseException("Failed to create YAML file: " + baseFile.getAbsolutePath(), e);
            }
            return;
        }

        try {
            Map<String, Object> root = mapper.readValue(baseFile, Map.class);

            Object targetSection = (rootKey != null)
                    ? root.get(rootKey)
                    : root;

            if (!(targetSection instanceof Map)) {
                throw new YamlDatabaseException("Root key '" + rootKey + "' is missing or invalid in file " + baseFile.getName());
            }

            // Serialize that section back to YAML and re-parse it as a typed map
            String subYaml = mapper.writeValueAsString(targetSection);

            Map<String, O> loadedMap = mapper.readValue(
                    subYaml,
                    mapper.getTypeFactory().constructMapType(Map.class, String.class, type)
            );

            if (loadedMap == null || loadedMap.isEmpty()) {
                System.err.println("Warning: Empty YAML section in " + baseFile.getAbsolutePath());
                return;
            }

            map.clear();
            map.putAll(loadedMap);
            loaded = true;

        } catch (IOException e) {
            loaded = false;
            throw new YamlDatabaseException("Error reading YAML file: " + baseFile.getAbsolutePath(), e);
        }
    }

    @Override
    public @Nullable O fetch(String key) {
        if (!loaded) refreshDatabase();
        if (map.isEmpty()) throw new YamlDatabaseException("YAML database is empty.");
        return map.get(key);
    }

    @Override
    public List<O> fetchAll() {
        if (!loaded) refreshDatabase();
        if (map.isEmpty()) throw new YamlDatabaseException("YAML database is empty.");
        return List.copyOf(map.values());
    }

    public CompletableFuture<O> fetchAsync(String key) {
        return CompletableFuture.supplyAsync(() -> fetch(key));
    }

    public CompletableFuture<List<O>> fetchAllAsync() {
        return CompletableFuture.supplyAsync(this::fetchAll);
    }
}
