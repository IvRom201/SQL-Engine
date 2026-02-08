package server.dto;

import java.util.List;

public class QueryResponse {
    public String kind;
    public String message;
    public Integer affectedRows;

    public List<ColumnDto> columns;
    public List<List<Object>> rows;
}
