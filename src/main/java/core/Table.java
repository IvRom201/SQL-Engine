package core;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Table {
    private final String tableName;
    private final List<Column> columns;
    private final List<Row> rows;

    public Table(String tableName, List<Column> columns) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.columns = new ArrayList<>(Objects.requireNonNull(columns, "columns"));
        this.rows = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public List<Row> getRows() {
        return rows;
    }

    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public void insert(Map<String, Object> rawValues) {
        Map<String, Value<?>> vs = new HashMap<>();
        for (Column c : columns) {
            Object raw = rawValues.get(c.getColumnName());
            Value<?> v = coerceToValue(c, raw);
            vs.put(c.getColumnName(), v);
        }
        rows.add(new Row(vs));
    }

    public void addRow(Row row) {
        rows.add(row);
    }

    public List<Row> select(Predicate<Row> predicate) {
        return rows.stream().filter(predicate).collect(Collectors.toList());
    }

    public int update(Predicate<Row> predicate, Map<String, Object> newValues) {
        int count = 0;
        for (Row r : rows) {
            if (predicate.test(r)) {
                for (Map.Entry<String, Object> e : newValues.entrySet()) {
                    Column col = findColumn(e.getKey());
                    if (col == null) continue;
                    r.setValue(col.getColumnName(), coerceToValue(col, e.getValue()));
                }
                count++;
            }
        }
        return count;
    }

    public int delete(Predicate<Row> predicate) {
        int before = rows.size();
        rows.removeIf(predicate);
        return before - rows.size();
    }

    private Column findColumn(String name) {
        for (Column c : columns) {
            if (c.getColumnName().equals(name)) return c;
        }
        return null;
    }

    private Value<?> coerceToValue(Column col, Object raw) {
        DataType t = col.getColumnType();
        if (raw == null) return new Value<>(null, t);

        try {
            return switch (t) {
                case INTEGER -> new Value<>(toInteger(raw), DataType.INTEGER);
                case DOUBLE  -> new Value<>(toDouble(raw), DataType.DOUBLE);
                case BOOLEAN -> new Value<>(toBoolean(raw), DataType.BOOLEAN);
                case STRING  -> new Value<>(String.valueOf(raw), DataType.STRING);
            };
        } catch (Exception ex) {
            return new Value<>(String.valueOf(raw), DataType.STRING);
        }
    }

    private Integer toInteger(Object o) {
        if (o instanceof Integer i) return i;
        if (o instanceof Number n) return n.intValue();
        return Integer.parseInt(String.valueOf(o));
    }

    private Double toDouble(Object o) {
        if (o instanceof Double d) return d;
        if (o instanceof Number n) return n.doubleValue();
        return Double.parseDouble(String.valueOf(o));
    }

    private Boolean toBoolean(Object o) {
        if (o instanceof Boolean b) return b;
        return Boolean.parseBoolean(String.valueOf(o));
    }

    public void addColumn(Column column) {
        Objects.requireNonNull(column, "column");
        this.columns.add(column);
        for (Row r : rows) {
            r.setValue(column.getColumnName(), new Value<>(null, column.getColumnType()));
        }
    }

}
