package dev.charcoal.database.bridge.file.exception;

public class JsonDatabaseException extends RuntimeException {

    public JsonDatabaseException(String message, Exception cause   ) {
        super(message, cause);
    }

    public JsonDatabaseException(String message) {
        super(message);
    }

}
