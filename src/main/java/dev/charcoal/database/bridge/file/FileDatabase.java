package dev.charcoal.database.bridge.file;


import dev.charcoal.database.bridge.SyncDatabase;

import java.time.Duration;

public abstract class FileDatabase<O> implements SyncDatabase<O> {

    protected abstract void refreshDatabase();

    @Deprecated
    @Override
    public boolean save(String key, O value) {
        return false;
    }

    @Deprecated
    @Override
    public boolean save(String key, O value, Duration timeout) {
        return false;
    }

    @Deprecated
    @Override
    public boolean delete(String key) {
        return false;
    }
}
