package core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Row {
    private final Map<String, Value<?>> values;

    public Row(Map<String, Value<?>> values) {
        this.values = new HashMap<>(values);
    }

    public void setValue(String columnName, Value<?> value) {
        values.put(columnName, value);
    }

    public Value<?> getValue(String columnName) {
        return values.get(columnName);
    }

    public Map<String, Value<?>> getValues() {
        return Collections.unmodifiableMap(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
