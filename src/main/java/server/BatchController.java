package server;

import org.springframework.web.bind.annotation.*;
import server.dto.BatchRequest;
import server.dto.BatchResponse;

@RestController
@RequestMapping
public class BatchController {
    private final SqlEngineService engine;

    public BatchController(SqlEngineService engine) {
        this.engine = engine;
    }

    @PostMapping("/batch")
    public BatchResponse batch(@RequestParam String sessionId, @RequestBody BatchRequest req) {
        return engine.executeBatch(sessionId, req);
    }
}
