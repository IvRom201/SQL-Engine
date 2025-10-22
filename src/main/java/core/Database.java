package core;

import java.util.*;

public class Database {
    private final Map<String, Table> tables = new HashMap<>();

    public void createTable(String tableName, List<Column> columns) {
        if (tables.containsKey(tableName)) {
            throw new RuntimeException("Table " + tableName + " already exists");
        }
        tables.put(tableName, new Table(tableName, columns));
    }

    public void dropTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new RuntimeException("No table named " + tableName);
        }
        tables.remove(tableName);
    }

    public Table getTable(String tableName) {
        Table t = tables.get(tableName);
        if (t == null) throw new RuntimeException("No table named " + tableName);
        return t;
    }

    public Set<String> listTables() {
        return Collections.unmodifiableSet(tables.keySet());
    }

    public void insert(String tableName, Map<String, Object> values) {
        getTable(tableName).insert(values);
    }
}
