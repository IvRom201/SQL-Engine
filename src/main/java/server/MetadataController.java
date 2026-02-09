package server;

import core.Table;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import server.dto.ColumnSchemaDto;
import server.dto.TableSchemaDto;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping
public class MetadataController {
    private final SqlEngineService engine;
    public MetadataController(SqlEngineService engine) {
        this.engine = engine;
    }

    @GetMapping("/tables")
    public Set<String> listTables() {
        return engine.listTables();
    }

    @GetMapping("/tables{name}")
    public TableSchemaDto describe(@PathVariable String name) {
        Table t = engine.getTableOrThrow(name);

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
