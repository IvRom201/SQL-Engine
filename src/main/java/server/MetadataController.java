package server;

import core.Table;
import org.springframework.web.bind.annotation.*;
import server.dto.ColumnSchemaDto;
import server.dto.TableSchemaDto;

import java.util.Set;

@RestController
@RequestMapping
public class MetadataController {
    private final SqlEngineService engine;

    public MetadataController(SqlEngineService engine) {
        this.engine = engine;
    }

    @GetMapping("/tables")
    public Set<String> listTables(@RequestParam String sessionId) {
        return engine.listTables(sessionId);
    }

    @GetMapping("/tables{name}")
    public TableSchemaDto describe(@RequestParam String sessionId, @PathVariable String name) {
        Table t = engine.getTableOrThrow(sessionId, name);

        var cols = t.getColumns().stream()
                .map(c -> new ColumnSchemaDto(
                        c.getColumnName(),
                        String.valueOf(c.getColumnType()),
                        c.isPrimaryKey()
                ))
                .toList();

        return new TableSchemaDto(t.getTableName(), cols);
    }

}
