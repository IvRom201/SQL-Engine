package server;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
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
    public BatchResponse batch(@RequestBody BatchRequest req) {
        return engine.executeBatch(req);
    }
}
