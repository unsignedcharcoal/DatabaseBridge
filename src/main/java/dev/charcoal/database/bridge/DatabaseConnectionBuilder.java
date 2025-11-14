package dev.charcoal.database.bridge;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

@Getter
@AllArgsConstructor @Builder
public class DatabaseConnectionBuilder {

    private String ip;
    private int port;
    private String database;
    private String username;
    private String password;
    private String table;

    @Contract("_, _, _, _ -> new")
    public static @NotNull DatabaseConnectionBuilder localhost(String database, String username, String password, String table) {
        return new DatabaseConnectionBuilder("localhost", 3306, database, username, password, table);
    }

    @Contract("_, _ -> new")
    public static @NotNull DatabaseConnectionBuilder localhost(String database, String table) {
        return new DatabaseConnectionBuilder("localhost", 3306, database, "root", "root", table);
    }

    public static @NotNull DatabaseConnectionBuilder localhost(int port ,String database, String table) {
        return new DatabaseConnectionBuilder("localhost", port, database, "root", "root", table);
    }

    public static @NotNull DatabaseConnectionBuilder redisLocalhost() {
        return new DatabaseConnectionBuilder("localhost", 6379, null, "root", "root", null);
    }

    @Contract("_, _, _, _, _, _ -> new")
    public static @NotNull DatabaseConnectionBuilder of(String ip, int port, String database, String username, String password, String table) {
        return new DatabaseConnectionBuilder(ip, port, database, username, password, table);
    }

    public String getSQLUrl() {
        return "jdbc:mysql://" + ip + ":" + port + "/" + database;
    }

    public String getMongoURL() {
        return "mongodb://" + ip + ":" + port;
    }

    public String getRedisURL() {
        return "redis://" + ip + ":" + port;
    }

}
