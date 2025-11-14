package dev.charcoal.database.bridge;

import java.time.Duration;
import java.util.List;

public interface SyncDatabase<T> {

    T fetch(String key);

    List<T> fetchAll();

    boolean save(String key, T value);

    boolean save(String key, T value, Duration timeout);

    boolean delete(String key);

}
