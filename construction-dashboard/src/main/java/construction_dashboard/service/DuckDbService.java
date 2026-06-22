package construction_dashboard.service;

import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
public class DuckDbService {

    private static final String DB_PATH =
            "../../DATA OBSERVABILITY/construction_elt_project/dev.duckdb";

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:" + DB_PATH);
    }

    public List<String> getTables() throws SQLException {
        List<String> tables = new ArrayList<>();

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }

        return tables;
    }

    public List<Map<String, Object>> previewTable(String tableName) throws SQLException {
        String sql = "SELECT * FROM " + tableName + " LIMIT 100";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            return resultSetToList(rs);
        }
    }

    public List<Map<String, Object>> getSiteHealthSummary() throws SQLException {
        String sql = "SELECT * FROM mart_site_health_summary";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            return resultSetToList(rs);
        }
    }

    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();

        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();

            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), rs.getObject(i));
            }

            rows.add(row);
        }

        return rows;
    }
}