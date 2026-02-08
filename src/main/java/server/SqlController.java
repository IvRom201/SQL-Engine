package server;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
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
    public QueryResponse query(@RequestBody QueryRequest req) {
        return engine.execute(req.sql);
    }
}
