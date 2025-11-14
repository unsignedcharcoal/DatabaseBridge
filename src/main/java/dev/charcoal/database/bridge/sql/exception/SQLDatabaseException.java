package dev.charcoal.database.bridge.sql.exception;

public class SQLDatabaseException extends RuntimeException {

    public SQLDatabaseException(String message) {
        super(message);
    }

    public SQLDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
