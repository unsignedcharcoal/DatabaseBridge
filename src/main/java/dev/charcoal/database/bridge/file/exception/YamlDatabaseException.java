package dev.charcoal.database.bridge.file.exception;

public class YamlDatabaseException extends RuntimeException {

    public YamlDatabaseException(String message, Exception cause   ) {
        super(message, cause);
    }

    public YamlDatabaseException(String message) {
        super(message);
    }

}
