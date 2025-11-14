package dev.charcoal.database.bridge.sql.impl;

import dev.charcoal.database.bridge.DatabaseConnectionBuilder;
import dev.charcoal.database.bridge.sql.SQLDatabase;
import dev.charcoal.database.bridge.sql.SQLQueryBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SQLiteDatabase<T> extends SQLDatabase<T> {

    private final String table;
    private final Function<ResultSet, T> mapper;

    public SQLiteDatabase(DatabaseConnectionBuilder builder, Class<T> clazz, Function<ResultSet, T> mapper) {
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
            SQLQueryBuilder qb = new SQLQueryBuilder()
                    .insertInto(table, "id, data")
                    .values("?, ?");

            String sql = qb.build().replaceFirst("INSERT", "INSERT OR REPLACE");

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, key);
                ps.setObject(2, value);
                ps.executeUpdate();
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
