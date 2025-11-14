package dev.charcoal.database.bridge.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.charcoal.database.bridge.AsyncDatabase;
import dev.charcoal.database.bridge.DataUtils;
import dev.charcoal.database.bridge.SyncDatabase;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RedisDatabase<T> implements AsyncDatabase<T>, SyncDatabase<T> {

    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> syncCommands;
    private final RedisAsyncCommands<String, String> asyncCommands;
    private final ObjectMapper mapper = DataUtils.mapper;
    private final Class<T> typeClass;

    public RedisDatabase(@NotNull String redisUrl, @NotNull Class<T> typeClass) {
        this(RedisClient.create(redisUrl), typeClass);
    }

    public RedisDatabase(@NotNull RedisClient redisClient, @NotNull Class<T> typeClass) {
        this.client = redisClient;
        this.connection = redisClient.connect();
        this.syncCommands = connection.sync();
        this.asyncCommands = connection.async();
        this.typeClass = typeClass;
    }


    @Override
    public T fetch(String key) {
        try {
            String json = syncCommands.get(key);
            return json == null ? null : mapper.readValue(json, typeClass);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch key: " + key, e);
        }
    }

    @Override
    public List<T> fetchAll() {
        throw new UnsupportedOperationException("Use fetchAll(prefix) or HSCAN to get all keys by pattern.");
    }

    public List<T> fetchAll(String prefix) {
        List<T> list = new ArrayList<>();
        try {
            for (String key : syncCommands.keys(prefix + "*")) {
                String json = syncCommands.get(key);
                if (json != null) list.add(mapper.readValue(json, typeClass));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch keys with prefix: " + prefix, e);
        }
        return list;
    }

    @Override
    public boolean save(String key, T value) {
        try {
            String json = mapper.writeValueAsString(value);
            syncCommands.set(key, json);
            return true;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object for key: " + key, e);
        }
    }

    @Override
    public boolean save(String key, T value, Duration timeout) {
        try {
            String json = mapper.writeValueAsString(value);
            syncCommands.setex(key, timeout.toSeconds(), json);
            return true;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object for key: " + key, e);
        }
    }

    @Override
    public boolean delete(String key) {
        return syncCommands.del(key) > 0;
    }

    @Override
    public CompletableFuture<T> fetchAsync(String key) {
        return asyncCommands.get(key)
                .thenApply(json -> {
                    if (json == null) return null;
                    try {
                        return mapper.readValue(json, typeClass);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Failed to deserialize object for key: " + key, e);
                    }
                })
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<List<T>> fetchAllAsync() {
        throw new UnsupportedOperationException("Use fetchAllAsync(prefix) for pattern-based fetching.");
    }

    public CompletableFuture<List<T>> fetchAllAsync(String prefix) {
        return asyncCommands.keys(prefix + "*")
                .thenCompose(keys -> {
                    List<CompletableFuture<T>> futures = new ArrayList<>();
                    for (String key : keys) {
                        futures.add(fetchAsync(key));
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> futures.stream()
                                    .map(CompletableFuture::join)
                                    .toList());
                })
                .toCompletableFuture();
    }

    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value) {
        try {
            String json = mapper.writeValueAsString(value);
            return asyncCommands.set(key, json)
                    .thenApply("OK"::equalsIgnoreCase)
                    .toCompletableFuture();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object for key: " + key, e);
        }
    }

    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value, Duration timeout) {
        try {
            String json = mapper.writeValueAsString(value);
            return asyncCommands.setex(key, timeout.toSeconds(), json)
                    .thenApply("OK"::equalsIgnoreCase)
                    .toCompletableFuture();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object for key: " + key, e);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(String key) {
        return asyncCommands.del(key)
                .thenApply(result -> result > 0)
                .toCompletableFuture();
    }

    public void close() {
        connection.close();
        client.shutdown();
    }
}
