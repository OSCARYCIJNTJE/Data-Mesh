package construction_dashboard.api;

import construction_dashboard.service.DuckDbService;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.*;

@RestController
@RequestMapping("/api/duckdb")
public class DuckDbController {

    private final DuckDbService duckDbService;

    public DuckDbController(DuckDbService duckDbService) {
        this.duckDbService = duckDbService;
    }

    @GetMapping("/tables")
    public List<String> getTables() throws SQLException {
        return duckDbService.getTables();
    }

    @GetMapping("/tables/{tableName}")
    public List<Map<String, Object>> previewTable(@PathVariable String tableName) throws SQLException {
        return duckDbService.previewTable(tableName);
    }

    @GetMapping("/site-health-summary")
    public List<Map<String, Object>> getSiteHealthSummary() throws SQLException {
        return duckDbService.getSiteHealthSummary();
    }
}