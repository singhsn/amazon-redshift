package com.example.redshift;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Utility helpers for running external SQL scripts located in classpath. */
public final class SqlUtils {

    private SqlUtils() { }

    public static void runScript(Connection con, String resourcePath) throws SQLException, IOException {
        try (InputStream in = SqlUtils.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Cannot find resource " + resourcePath);
            }
            String sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            // Split on semicolon at line end — fairly safe for our simple files
            String[] statements = sql.split(";\s*(?=\n|$)");
            try (Statement stmt = con.createStatement()) {
                for (String s : statements) {
                    String trimmed = s.trim();
                    if (!trimmed.isEmpty()) {
                        stmt.execute(trimmed);
                    }
                }
            }
        }
        con.commit();
    }
}
