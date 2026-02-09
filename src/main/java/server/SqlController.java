package server;

import org.springframework.web.bind.annotation.*;
import server.dto.QueryRequest;
import server.dto.QueryResponse;

@RestController
@RequestMapping("/api")
public class SqlController {
    private final SqlEngineService engine;

    public SqlController(SqlEngineService engine) {
        this.engine = engine;
    }

    @PostMapping("/query")
    public QueryResponse query(@RequestParam String sessionId, @RequestBody QueryRequest req) {
        return engine.execute(sessionId, req.sql());
    }
}
