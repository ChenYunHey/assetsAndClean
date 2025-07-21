package com.lakesoul.newClean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class PGConnectionPool {


    private static HikariDataSource dataSource;

    public static void init(String jdbcUrl, String user, String password) throws ClassNotFoundException {
        if (dataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(user);
            config.setPassword(password);

            config.setMaximumPoolSize(10);       // 可调，根据并发数
            config.setMinimumIdle(1);
            config.setIdleTimeout(60000);        // 空闲连接超时时间
            config.setConnectionTimeout(10000);  // 获取连接超时时间
            Class.forName("org.postgresql.Driver"); // 确保驱动类加载
            dataSource = new HikariDataSource(config);
            System.out.println("✅ HikariCP PostgreSQL pool initialized.");
        }
    }

    public static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new IllegalStateException("PG pool not initialized!");
        }
        return dataSource.getConnection();
    }

    public static void close() {
        if (dataSource != null) {
            dataSource.close();
            System.out.println("✅ HikariCP pool closed.");
        }
    }
}
