package dev.charcoal.database.bridge.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.charcoal.database.bridge.AsyncDatabase;
import dev.charcoal.database.bridge.DatabaseConnectionBuilder;
import dev.charcoal.database.bridge.SyncDatabase;
import dev.charcoal.database.bridge.sql.annotations.Column;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class SQLDatabase<T> implements AsyncDatabase<T>, SyncDatabase<T> {

    private final HikariDataSource dataSource;
    private final Class<T> type;
    private final String tableName;

    public SQLDatabase(@NotNull DatabaseConnectionBuilder connectionBuilder, Class<T> type) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionBuilder.getSQLUrl());
        config.setUsername(connectionBuilder.getUsername());
        config.setPassword(connectionBuilder.getPassword());

        this.tableName = connectionBuilder.getTable();
        this.type = type;

        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(10000); // 10s
        config.setIdleTimeout(60000);       // 1 min
        config.setMaxLifetime(1800000);     // 30 min
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        this.dataSource = new HikariDataSource(config);
        createTableIfMissing();
    }

    protected Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    private void createTableIfMissing() {
        String ddl = generateCreateTableDDL();
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(ddl);
            System.out.println("[SQLDatabase] Ensured table: " + tableName);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create table for " + tableName, e);
        }
    }

    private String generateCreateTableDDL() {
        List<String> columns = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Column.class)) continue;

            Column col = field.getAnnotation(Column.class);
            String sqlType = !col.type().isEmpty() ? col.type() : mapJavaTypeToSQL(field.getType());
            String colDef = col.name() + " " + sqlType + (col.id() ? " PRIMARY KEY" : "");
            columns.add(colDef);
        }

        String columnDefs = String.join(", ", columns);
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + columnDefs + ");";
    }

    private String mapJavaTypeToSQL(Class<?> clazz) {
        if (clazz == String.class) return "VARCHAR(255)";
        if (clazz == int.class || clazz == Integer.class) return "INT";
        if (clazz == long.class || clazz == Long.class) return "BIGINT";
        if (clazz == boolean.class || clazz == Boolean.class) return "BOOLEAN";
        if (clazz == double.class || clazz == Double.class) return "DOUBLE";
        if (clazz == float.class || clazz == Float.class) return "FLOAT";
        return "TEXT";
    }


    // Abstract hooks to be implemented by subclasses
    protected abstract T mapResult(ResultSet rs) throws SQLException;

    protected abstract void saveToDatabase(String key, T value) throws SQLException;

    protected abstract List<T> loadAll() throws SQLException;

    protected abstract T load(String key) throws SQLException;

    protected abstract boolean deleteFromDatabase(String key) throws SQLException;

    /* ------------------ SYNC ------------------ */

    @Override
    public T fetch(String key) {
        try {
            return load(key);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<T> fetchAll() {
        try {
            return loadAll();
        } catch (SQLException e) {
            e.printStackTrace();
            return List.of();
        }
    }

    @Override
    public boolean save(String key, T value) {
        try {
            saveToDatabase(key, value);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            return deleteFromDatabase(key);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public CompletableFuture<T> fetchAsync(String key) {
        return CompletableFuture.supplyAsync(() -> fetch(key));
    }

    @Override
    public CompletableFuture<List<T>> fetchAllAsync() {
        return CompletableFuture.supplyAsync(this::fetchAll);
    }

    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value) {
        return CompletableFuture.supplyAsync(() -> save(key, value));
    }

    @Override
    public CompletableFuture<Boolean> deleteAsync(String key) {
        return CompletableFuture.supplyAsync(() -> delete(key));
    }

    @Deprecated
    @Override
    public CompletableFuture<Boolean> saveAsync(String key, T value, Duration timeout) {
        return null;
    }

    @Deprecated
    @Override
    public boolean save(String key, T value, Duration timeout) {
        return false;
    }

}
