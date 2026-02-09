package server.dto;

import java.util.List;

public record TableSchemaDto (String name, List<ColumnSchemaDto> columns) {}
