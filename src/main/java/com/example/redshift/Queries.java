package com.example.redshift;

import java.sql.*;

/** Encapsulates the three analytic queries required by Assignment‑6. */
public class Queries {

    private final Connection con;

    public Queries(Connection con) {
        this.con = con;
    }

    /**
     * Returns the 10 most recent orders (total sale & date) for customers in America.
     */
    public ResultSet query1() throws SQLException {
        String sql = """
                SELECT o.o_orderkey     AS order_key,
                       o.o_totalprice   AS total_sale,
                       o.o_orderdate    AS order_date
                FROM   orders o
                       JOIN customer c ON o.o_custkey = c.c_custkey
                       JOIN nation   n ON c.c_nationkey = n.n_nationkey
                       JOIN region   r ON n.n_regionkey = r.r_regionkey
                WHERE  r.r_name = 'AMERICA'
                ORDER  BY o.o_orderdate DESC
                LIMIT  10
                """;
        PreparedStatement ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                    ResultSet.CONCUR_READ_ONLY);
        return ps.executeQuery();
    }

    /**
     * Returns customer key and total spend (desc) for urgent, non‑failed orders made
     * by customers outside Europe belonging to the largest market segment.
     */
    public ResultSet query2() throws SQLException {
        String sql = """
           WITH largest AS (
             SELECT c_mktsegment seg
             FROM customer
             GROUP BY c_mktsegment
             ORDER BY COUNT(*) DESC
             LIMIT 1
           )
           SELECT c.c_custkey,
                  SUM(o.o_totalprice) AS total_spent
           FROM   orders o
                  JOIN customer c   ON o.o_custkey = c.c_custkey
                  JOIN nation   n   ON c.c_nationkey = n.n_nationkey
                  JOIN region   r   ON n.n_regionkey = r.r_regionkey
                  JOIN largest  l   ON c.c_mktsegment = l.seg
           WHERE  o.o_orderpriority LIKE '1-URGENT%'
             AND  o.o_orderstatus   <> 'F'
             AND  r.r_name          <> 'EUROPE'
           GROUP  BY c.c_custkey
           ORDER  BY total_spent DESC
        """;
        PreparedStatement ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                    ResultSet.CONCUR_READ_ONLY);
        return ps.executeQuery();
    }

    /**
     * Returns count of lineitems ordered between 1997‑04‑01 and 2003‑04‑01 (6 years)
     * grouped by order priority ascending.
     */
    public ResultSet query3() throws SQLException {
        String sql = """
            SELECT o.o_orderpriority AS order_priority,
                   COUNT(*)          AS lineitem_count
            FROM   lineitem l
                   JOIN orders o ON l.l_orderkey = o.o_orderkey
            WHERE  o.o_orderdate >= DATE '1997‑04‑01'
              AND  o.o_orderdate <  DATE '2003‑04‑01'
            GROUP  BY o.o_orderpriority
            ORDER  BY o.o_orderpriority
        """;
        PreparedStatement ps = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                    ResultSet.CONCUR_READ_ONLY);
        return ps.executeQuery();
    }
}
