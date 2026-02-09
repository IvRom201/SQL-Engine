package server.dto;

import java.util.List;

public record QueryResponse(
        String kind,
        String message,
        Integer affectedRows,
        List<ColumnDto> columns,
        List<List<Object>> rows
) {
    public static QueryResponse update(String message, int affectedRows) {
        return new QueryResponse("UPDATE_COUNT", message, affectedRows, null, null);
    }

    public static QueryResponse result(List<ColumnDto> columns, List<List<Object>> rows) {
        return new QueryResponse("RESULT_SET", "OK", null, columns, rows);
    }

    public static QueryResponse error(String message) {
        return new QueryResponse("ERROR", message, null, null, null);
    }
}
