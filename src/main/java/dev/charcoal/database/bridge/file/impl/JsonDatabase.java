package dev.charcoal.database.bridge.file.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.charcoal.database.bridge.file.FileDatabase;
import dev.charcoal.database.bridge.file.exception.JsonDatabaseException;
import dev.charcoal.database.bridge.utils.DataUtils;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class JsonDatabase<O> extends FileDatabase<O> {

    private final File baseFile;
    private final ObjectMapper mapper;

    private final Map<String, O> map;

    @Getter
    private boolean loaded = false;

    private final Class<O> type;

    public JsonDatabase(File baseFile, Class<O> type) {
        this.baseFile = baseFile;
        this.mapper = DataUtils.mapper;
        this.map = new HashMap<>();
        this.type = type;
    }

    @Override
    protected void refreshDatabase() {
        if (!baseFile.exists()) {
            System.err.println("File does not exist!");
            return;
        }

        try {
            Map<String, O> resultMap = mapper.readValue(
                    baseFile,
                    mapper.getTypeFactory().constructMapType(Map.class, String.class, type)
            );

            map.clear();
            map.putAll(resultMap);
            loaded = true;
        } catch (IOException e) {
            loaded = false;
            throw new JsonDatabaseException("Error reading file: " + baseFile.getAbsolutePath(), e);
        }
    }

    @Override
    public O fetch(String key) {
        if (!loaded) refreshDatabase();
        if (map.isEmpty()) throw new JsonDatabaseException("Map is empty.");

        return map.get(key);
    }

    @Override
    public List<O> fetchAll() {
        if (!loaded) refreshDatabase();
        if (map.isEmpty()) throw new JsonDatabaseException("Map is empty.");

        return List.copyOf(map.values());
    }

    public CompletableFuture<O> fetchAsync(String key) {
        return CompletableFuture.supplyAsync(() -> fetch(key));
    }

    public CompletableFuture<List<O>> fetchAllAsync() {
        return CompletableFuture.supplyAsync(this::fetchAll);
    }

}
