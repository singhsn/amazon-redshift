package com.example.redshift;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Manages JDBC connection life‑cycle for Amazon Redshift.
 * <p>
 * Update the URL/UID/PW constants below with your cluster’s endpoint,
 * database name, and credentials. You can also override them with the
 * environment variables REDSHIFT_URL, REDSHIFT_UID and REDSHIFT_PW.
 */
public final class ConnectionManager {
    private static Connection connection;

    private static final String URL =
            System.getenv().getOrDefault("REDSHIFT_URL",
                    "jdbc:postgresql://redshift-cluster-1.c5lrlt7ktuwp.us-east-1.redshift.amazonaws.com:5439/dev");
    private static final String UID =
            System.getenv().getOrDefault("REDSHIFT_UID", "admin");
    private static final String PW =
            System.getenv().getOrDefault("REDSHIFT_PW", "sachchida@123");

    private ConnectionManager() { }

    public static Connection get() throws SQLException {
        if (connection == null || connection.isClosed()) {
            System.out.println("Connecting to Redshift…");
            connection = DriverManager.getConnection(URL, UID, PW);
            connection.setAutoCommit(false);
        }
        return connection;
    }

    public static void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Connection closed.");
            } catch (SQLException ignored) { }
        }
    }
}
