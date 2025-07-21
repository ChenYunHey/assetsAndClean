import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLDemo {

    // 数据库连接信息 - 请根据你的环境修改
    private static final String URL = "jdbc:postgresql://localhost:5432/lakesoul_test";
    private static final String USER = "lakesoul_test";
    private static final String PASSWORD = "lakesoul_test";

    public static void main(String[] args) {
        Connection connection = null;

        try {
            // 1. 加载驱动 (新版本JDBC可以自动加载，这行可以省略)
            Class.forName("org.postgresql.Driver");

            // 2. 建立连接
            System.out.println("Connecting to PostgreSQL database...");
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connection established successfully!");

            // 3. 创建表 (如果不存在)
            createTable(connection);

            // 4. 插入数据
            insertData(connection, 1, "John Doe", "john@example.com");
            insertData(connection, 2, "Jane Smith", "jane@example.com");

            // 5. 查询数据
            queryData(connection);

            // 6. 更新数据
            updateData(connection, 1, "John Updated");

            // 7. 再次查询验证更新
            queryData(connection);

            // 8. 删除数据
            deleteData(connection, 2);

            // 9. 最终查询
            queryData(connection);

        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC driver not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Connection failed!");
            e.printStackTrace();
        } finally {
            // 10. 关闭连接
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("Connection closed.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id SERIAL PRIMARY KEY, " +
                "name VARCHAR(100) NOT NULL, " +
                "email VARCHAR(100) UNIQUE NOT NULL)";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Table created (if not exists)");
        }
    }

    private static void insertData(Connection conn, int id, String name, String email) throws SQLException {
        String sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            pstmt.setString(2, name);
            pstmt.setString(3, email);
            pstmt.executeUpdate();
            System.out.println("Data inserted: " + name);
        }
    }

    private static void queryData(Connection conn) throws SQLException {
        String sql = "SELECT * FROM users";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\nCurrent data in users table:");
            System.out.println("ID\tName\t\tEmail");
            System.out.println("--------------------------------");

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String email = rs.getString("email");

                System.out.printf("%d\t%s\t%s%n", id, name, email);
            }
        }
    }

    private static void updateData(Connection conn, int id, String newName) throws SQLException {
        String sql = "UPDATE users SET name = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, newName);
            pstmt.setInt(2, id);
            int rowsAffected = pstmt.executeUpdate();
            System.out.println("\nUpdated " + rowsAffected + " row(s)");
        }
    }

    private static void deleteData(Connection conn, int id) throws SQLException {
        String sql = "DELETE FROM users WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            int rowsAffected = pstmt.executeUpdate();
            System.out.println("\nDeleted " + rowsAffected + " row(s)");
        }
    }
}
