package dev.charcoal.database.bridge.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import dev.charcoal.database.bridge.AsyncDatabase;
import dev.charcoal.database.bridge.DatabaseConnectionBuilder;
import dev.charcoal.database.bridge.SyncDatabase;
import dev.charcoal.database.bridge.mongo.annotations.MongoId;
import dev.charcoal.database.bridge.mongo.exception.MongoDatabaseException;
import dev.charcoal.database.bridge.sql.annotations.Column;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoDatabase<T> implements AsyncDatabase<T>, SyncDatabase<T> {

    private static MongoClient sharedClient; //Global shared client
    private static CodecRegistry sharedCodecRegistry;

    private final MongoCollection<T> collection;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Field idField;
    private final String mongoIdName;

    public MongoDatabase(@NotNull MongoClient client, String databaseName, String collectionName, Class<T> clazz) {
        initSharedCodecRegistry(); // ensure codec registry initialized

        this.collection = client
                .getDatabase(databaseName)
                .withCodecRegistry(sharedCodecRegistry)
                .getCollection(collectionName, clazz);

        this.idField = findIdField(clazz);
        this.idField.setAccessible(true);
        this.mongoIdName = resolveIdName(idField);
    }

    public MongoDatabase(@NotNull DatabaseConnectionBuilder connectionBuilder, Class<T> clazz) {
        initSharedCodecRegistry();

        synchronized (MongoDatabase.class) {
            if (sharedClient == null) {
                MongoClientSettings settings = MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString(connectionBuilder.getMongoURL()))
                        .uuidRepresentation(UuidRepresentation.STANDARD)
                        .codecRegistry(sharedCodecRegistry)
                        .build();

                sharedClient = MongoClients.create(settings);
                System.out.println("[MongoDatabase] Shared MongoClient initialized");
            }
        }

        this.collection = sharedClient
                .getDatabase(connectionBuilder.getDatabase())
                .withCodecRegistry(sharedCodecRegistry)
                .getCollection(connectionBuilder.getTable(), clazz);

        this.idField = findIdField(clazz);
        this.idField.setAccessible(true);
        this.mongoIdName = resolveIdName(idField);
    }

    private static void initSharedCodecRegistry() {
        if (sharedCodecRegistry == null) {
            sharedCodecRegistry = fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build())
            );
        }
    }

    private String resolveIdName(Field idField) {
        if (idField.isAnnotationPresent(MongoId.class)) {
            String value = idField.getAnnotation(MongoId.class).value();
            return value.isEmpty() ? "_id" : value;
        } else if (idField.isAnnotationPresent(Column.class)) {
            return idField.getAnnotation(Column.class).name();
        }
        return "_id";
    }

    private Field findIdField(Class<T> clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(MongoId.class)) return field;
            if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).id()) return field;
        }
        throw new IllegalStateException("No @MongoId or @Column(id=true) found in class " + clazz.getName());
    }

    private Object getIdValue(T object) {
        try {
            return idField.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access id field", e);
        }
    }

    @Override
    public T fetch(String key) {
        try {
            return collection.find(Filters.eq(mongoIdName, key)).first();
        } catch (Exception e) {
            throw new MongoDatabaseException("Cannot find id field", e);
        }
    }

    @Override
    public List<T> fetchAll() {
        try {
            List<T> list = new ArrayList<>();
            collection.find().into(list);
            return list;
        } catch (Exception e) {
            throw new MongoDatabaseException("Cannot fetch all", e);
        }
    }

    @Override
    public boolean save(String key, T value) {
        try {
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            collection.replaceOne(Filters.eq(mongoIdName, key), value, options);
            return true;
        } catch (Exception e) {
            throw new MongoDatabaseException("Cannot save value", e);
        }
    }

    @Override
    public boolean save(String key, T value, Duration timeout) {
        return save(key, value);
    }


    @Override
    public boolean delete(String key) {
        try {
            return collection.deleteOne(Filters.eq(mongoIdName, key)).getDeletedCount() > 0;
        } catch (Exception e) {
            throw new MongoDatabaseException("Cannot delete document with id " + key, e);
        }
    }

    @Override
    public CompletableFuture<T> fetchAsync(String key) {
        return CompletableFuture.supplyAsync(() -> fetch(key), executorService);
    }

    @Override
    public CompletableFuture<List<T>> fetchAllAsync() {
        return CompletableFuture.supplyAsync(this::fetchAll, executorService);
    }

    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value) {
        return CompletableFuture.supplyAsync(() -> save(key, value), executorService);
    }

    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> save(key, value, timeout), executorService);
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(String key) {
        return CompletableFuture.supplyAsync(() -> delete(key), executorService);
    }

    public void close() {
        executorService.shutdownNow();
    }

    public MongoCollection<T> getCollection() {
        return collection;
    }

    public static void closeSharedClient() {
        if (sharedClient != null) {
            sharedClient.close();
            sharedClient = null;
            System.out.println("[MongoDatabase] Shared MongoClient closed");
        }
    }
}
