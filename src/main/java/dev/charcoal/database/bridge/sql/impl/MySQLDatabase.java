package dev.charcoal.database.bridge.sql.impl;

import dev.charcoal.database.bridge.DatabaseConnectionBuilder;
import dev.charcoal.database.bridge.sql.SQLDatabase;
import dev.charcoal.database.bridge.sql.SQLQueryBuilder;
import dev.charcoal.database.bridge.sql.annotations.Column;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MySQLDatabase<T> extends SQLDatabase<T> {

    private final String table;
    private final Function<ResultSet, T> mapper;

    public MySQLDatabase(DatabaseConnectionBuilder builder, Class<T> clazz, Function<ResultSet, T> mapper) {
        super(builder, clazz);
        this.table = builder.getTable();
        this.mapper = mapper;
    }

    @Override
    protected T mapResult(ResultSet rs) throws SQLException {
        return mapper.apply(rs);
    }


    @Override
    protected void saveToDatabase(String key, T value) throws SQLException {
        try (Connection conn = getConnection()) {
            List<Field> fields = Arrays.stream(value.getClass().getDeclaredFields())
                    .filter(f -> f.isAnnotationPresent(Column.class))
                    .toList();

            String columns = fields.stream()
                    .map(f -> f.getAnnotation(Column.class).name())
                    .collect(Collectors.joining(", "));

            String placeholders = fields.stream().map(f -> "?").collect(Collectors.joining(", "));

            //String sql = "REPLACE INTO " + table + " (" + columns + ") VALUES (" + placeholders + ");";
            String sql = new SQLQueryBuilder().replaceInto(table, columns, placeholders).build();

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < fields.size(); i++) {
                    Field f = fields.get(i);
                    f.setAccessible(true);
                    ps.setObject(i + 1, f.get(value));
                }
                ps.executeUpdate();
            } catch (IllegalAccessException e) {
                throw new SQLException("Failed to access field value", e);
            }
        }
    }


    @Override
    protected List<T> loadAll() throws SQLException {
        List<T> results = new ArrayList<>();
        try (Connection conn = getConnection()) {
            SQLQueryBuilder qb = new SQLQueryBuilder()
                    .select("*")
                    .from(table);

            try (PreparedStatement ps = conn.prepareStatement(qb.build());
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapResult(rs));
                }
            }
        }
        return results;
    }

    @Override
    protected T load(String key) throws SQLException {
        try (Connection conn = getConnection()) {
            SQLQueryBuilder qb = new SQLQueryBuilder()
                    .select("*")
                    .from(table)
                    .where("id = ?");

            try (PreparedStatement ps = conn.prepareStatement(qb.build())) {
                ps.setString(1, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return mapResult(rs);
                }
            }
        }
        return null;
    }

    @Override
    protected boolean deleteFromDatabase(String key) throws SQLException {
        try (Connection conn = getConnection()) {
            SQLQueryBuilder qb = new SQLQueryBuilder()
                    .deleteFrom(table)
                    .where("id = ?");

            try (PreparedStatement ps = conn.prepareStatement(qb.build())) {
                ps.setString(1, key);
                return ps.executeUpdate() > 0;
            }
        }
    }

}
