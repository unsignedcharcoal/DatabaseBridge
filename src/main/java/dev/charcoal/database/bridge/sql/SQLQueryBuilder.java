package dev.charcoal.database.bridge.sql;

public class SQLQueryBuilder {

    private final StringBuilder query;

    public SQLQueryBuilder() {
        this.query = new StringBuilder();
    }

    public SQLQueryBuilder select(String columns) {
        query.append("SELECT ").append(columns).append(" ");
        return this;
    }

    public SQLQueryBuilder from(String table) {
        query.append("FROM ").append(table).append(" ");
        return this;
    }

    public SQLQueryBuilder where(String condition) {
        query.append("WHERE ").append(condition).append(" ");
        return this;
    }

    public SQLQueryBuilder and(String condition) {
        query.append("AND ").append(condition).append(" ");
        return this;
    }

    public SQLQueryBuilder or(String condition) {
        query.append("OR ").append(condition).append(" ");
        return this;
    }

    public SQLQueryBuilder insertInto(String table, String columns) {
        query.append("INSERT INTO ").append(table)
                .append(" (").append(columns).append(") ");
        return this;
    }

    public SQLQueryBuilder replaceInto(String table, String columns,  String... values) {
        String valuesString = String.join(",", values);
        query.append("REPLACE INTO ").append(table)
                .append(" (").append(columns).append(") VALUES (").append(valuesString).append(");");
        return this;
    }

    public SQLQueryBuilder values(String values) {
        query.append("VALUES (").append(values).append(") ");
        return this;
    }

    public SQLQueryBuilder update(String table) {
        query.append("UPDATE ").append(table).append(" ");
        return this;
    }

    public SQLQueryBuilder set(String assignments) {
        query.append("SET ").append(assignments).append(" ");
        return this;
    }

    public SQLQueryBuilder deleteFrom(String table) {
        query.append("DELETE FROM ").append(table).append(" ");
        return this;
    }

    public String build() {
        return query.toString().trim() + ";";
    }

    public void clear() {
        query.setLength(0);
    }
}
