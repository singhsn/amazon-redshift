package com.example.redshift;

import java.io.IOException;
import java.sql.*;

public class AmazonRedshift {

    private final Connection con;
    private final Queries queries;

    public AmazonRedshift() throws SQLException {
        this.con = ConnectionManager.get();
        this.queries = new Queries(con);
    }

    /**
     * Drops all TPC‑H tables if present.
     */
    public void drop() throws SQLException, IOException {
        System.out.println("Dropping existing tables …");
        SqlUtils.runScript(con, "ddl/tpch_drop.sql");
    }

    /**
     * Creates schema & tables.
     */
    public void create() throws SQLException, IOException {
        System.out.println("Creating tables …");
        SqlUtils.runScript(con, "ddl/tpch_create.sql");
    }

    /**
     * Loads sample data from *.sql files inside resources/ddl
     */
    public void insert() throws SQLException, IOException {
        System.out.println("Ingesting TPC‑H sample data …");
        String[] dataFiles = {
                "ddl/customer.sql",
                "ddl/nation.sql",
                "ddl/region.sql",
                "ddl/supplier.sql",
                "ddl/part.sql",
                "ddl/partsupp.sql",
                "ddl/orders.sql",
                "ddl/lineitem.sql"
        };
        for (String f : dataFiles) {
            System.out.println("  → " + f);
            SqlUtils.runScript(con, f);
        }
    }

    /** Convenience wrapper to display ResultSet as CSV‑like output. */
    public static void print(ResultSet rs, int maxRows) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        // Header
        for (int i = 1; i <= cols; i++) {
            System.out.print(meta.getColumnLabel(i));
            if (i < cols) System.out.print(", ");
        }
        System.out.println();
        int count = 0;
        while (rs.next() && count < maxRows) {
            for (int i = 1; i <= cols; i++) {
                System.out.print(rs.getObject(i));
                if (i < cols) System.out.print(", ");
            }
            System.out.println();
            count++;
        }
        System.out.println("Displayed " + count + " of total results.");
    }

    public static void main(String[] args) {
        try {
            AmazonRedshift app = new AmazonRedshift();
            app.drop();
            app.create();
            app.insert();

            System.out.println("\nQuery‑1 Results:");
            print(app.queries.query1(), 10);

            System.out.println("\nQuery‑2 Results:");
            print(app.queries.query2(), 10);

            System.out.println("\nQuery‑3 Results:");
            print(app.queries.query3(), 10);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ConnectionManager.closeQuietly();
        }
    }
}
