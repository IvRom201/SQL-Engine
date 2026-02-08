package server;

import core.*;
import query_layer.Query;
import query_layer.QueryExecutor;
import query_layer.QueryParser;
import server.dto.ColumnDto;
import server.dto.QueryResponse;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlDispatcher {
    private static final Pattern RE_CREATE =
            Pattern.compile(
                    "^CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.+)\\)\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private static final Pattern RE_ALTER_ADD =
            Pattern.compile(
                    "^ALTER\\s+TABLE\\s+(\\w+)\\s+ADD\\s+COLUMN\\s+(\\w+)\\s+(STRING|INTEGER|DOUBLE|BOOLEAN)(\\s+PRIMARY\\s+KEY)?\\s*;?$",
                    Pattern.CASE_INSENSITIVE
            );

    private static final Pattern RE_DROP =
            Pattern.compile(
                    "^DROP\\s+TABLE\\s+(\\w+)\\s*;?$",
                    Pattern.CASE_INSENSITIVE
            );

    private static final Pattern RE_INSERT =
            Pattern.compile(
                    "^INSERT\\s+INTO\\s+(\\w+)\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

    private static final Pattern RE_UPDATE =
            Pattern.compile(
                    "^UPDATE\\s+(\\w+)\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+))?\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

    private static final Pattern RE_DELETE =
            Pattern.compile(
                    "^DELETE\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

    private final QueryParser parser = new QueryParser();
    private final QueryExecutor executor = new QueryExecutor();

    public QueryResponse execute(Database db, String sql){
        if (sql == null || sql.trim().isEmpty()){
            throw new IllegalArgumentException("Empty SQL command");
        }

        String s = sql.trim();
        String up = s.toUpperCase(Locale.ROOT);

        if (up.startsWith("SELECT ")) return handleSelect(db, s);
        if (up.startsWith("CREATE TABLE")) return handleCreate(db, s);
        if (up.startsWith("ALTER TABLE")) return handleAlterAdd(db, s);
        if (up.startsWith("DROP TABLE")) return handleDrop(db, s);
        if (up.startsWith("INSERT INTO")) return handleInsert(db, s);
        if (up.startsWith("UPDATE ")) return handleUpdate(db, s);
        if (up.startsWith("DELETE FROM")) return handleDelete(db, s);

        throw new IllegalArgumentException("Unknown SQL command");
    }

    private QueryResponse handleSelect(Database db, String sql){
        Query<Row> q = parser.parse(sql, db);
        List<Row> result = executor.execute(q);

        // если SELECT * -> selectedColumns пустой список
        List<String> cols = q.getSelectedColumns();
        if (cols == null || cols.isEmpty()) {
            cols = q.getTable().getColumns().stream().map(Column::getColumnName).toList();
        }

        Map<String, DataType> typeByCol = new HashMap<>();
        for (Column c : q.getTable().getColumns()) {
            typeByCol.put(c.getColumnName(), c.getColumnType());
        }

        QueryResponse resp = new QueryResponse();
        resp.kind = "RESULT_SET";
        resp.message = "OK";
        resp.columns = cols.stream()
                .map(c -> new ColumnDto(c, String.valueOf(typeByCol.getOrDefault(c, null))))
                .toList();

        List<List<Object>> rows = new ArrayList<>();
        for (Row r : result) {
            List<Object> row = new ArrayList<>(cols.size());
            for (String c : cols) {
                Value<?> v = r.getValue(c);
                row.add(v == null ? null : v.getRaw());
            }
            rows.add(row);
        }
        resp.rows = rows;
        return resp;
    }

    private QueryResponse handleCreate(Database db, String line){
        Matcher matcher = RE_CREATE.matcher(line);
        if (!matcher.matches()) throw new IllegalArgumentException("Usage: CREATE TABLE <name> (col TYPE [PRIMARY KEY], ...);");

        String tableName = matcher.group(1);
        String colsSpec = matcher.group(2).trim();

        String[] defs = splitByCommaRespectQuotes(colsSpec);
        List<Column> cols = new ArrayList<>();
        for (String def : defs) {
            String[] toks = def.trim().split("\\s+");
            if (toks.length < 2) throw new IllegalArgumentException("Invalid column definition: " + def);

            String colName = toks[0];
            DataType type = DataType.valueOf(toks[1].toUpperCase(Locale.ROOT));

            Column c = new Column(colName, type);
            if (toks.length >= 3) {
                String rest = String.join(" ", Arrays.copyOfRange(toks, 2, toks.length)).toUpperCase(Locale.ROOT);
                if (rest.contains("PRIMARY KEY")) c.setPrimaryKey(true);
            }
            cols.add(c);
        }

        db.createTable(tableName, cols);

        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = "Table created: " + tableName;
        resp.affectedRows = 0;
        return resp;
    }

    private QueryResponse handleAlterAdd(Database db, String line){
        Matcher matcher = RE_ALTER_ADD.matcher(line);
        if (!matcher.matches()) throw new IllegalArgumentException("Usage: ALTER TABLE <name> ADD COLUMN <col> <TYPE> [PRIMARY KEY];");

        String tableName = matcher.group(1);
        String col = matcher.group(2);
        DataType type = DataType.valueOf(matcher.group().toUpperCase(Locale.ROOT));
        boolean pk = matcher.group(4) != null;

        Table t = db.getTable(tableName);
        Column c = new Column(col, type);
        c.setPrimaryKey(pk);
        t.addColumn(c);

        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = "Column added: " + tableName + "." + col;
        resp.affectedRows = 0;
        return resp;
    }

    private QueryResponse handleDrop(Database db, String line){
        Matcher matcher = RE_DROP.matcher(line);
        if (!matcher.matches()) throw new IllegalArgumentException("Usage: DROP TABLE <name>");

        String tableName = matcher.group(1);
        db.dropTable(tableName);

        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = "Table dropped: " + tableName;
        resp.affectedRows = 0;
        return resp;
    }

    private QueryResponse handleInsert(Database db, String line){
        Matcher matcher = RE_INSERT.matcher(line);
        if (!matcher.matches()) throw new IllegalArgumentException("Usage: INSERT INTO <name>(c1,...) VALUES (v1,...);");

        String tableName = matcher.group(1);
        String cols = matcher.group(2);
        String vals = matcher.group(3);

        String[] colName = splitByCommaRespectQuotes(cols);
        String[] valLits  = splitByCommaRespectQuotes(vals);

        if (colName.length != valLits.length) throw new IllegalArgumentException("Columns count != values count");

        Table t = db.getTable(tableName);
        Map<String, Object> toInsert = new LinkedHashMap<>();

        for (int i = 0; i < colName.length; i++) {
            String col = colName[i].trim();
            String lit = valLits[i].trim();

            Column schemaCol = t.getColumns().stream()
                    .filter(c -> c.getColumnName().equals(col))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid column: " + col));

            toInsert.put(col, literalToTyped(lit, schemaCol.getColumnType()));
        }

        t.insert(toInsert);
        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = "1 row inserted into " + tableName;
        resp.affectedRows = 1;
        resp.columns = null;
        resp.rows = null;
        return resp;
    }

    private QueryResponse handleUpdate(Database db, String line){
        Matcher matcher = RE_UPDATE.matcher(line);
        if (matcher.matches()) throw new IllegalArgumentException("Usage: UPDATE <name> SET col=val[, ...] [WHERE ...];");

        String tableName = matcher.group(1);
        String setSpec = matcher.group(2).trim();
        String where = matcher.group(3);

        Table t = db.getTable(tableName);

        Map<String, Object> newVals = new LinkedHashMap<>();
        String[] pairs = splitByCommaRespectQuotes(setSpec);

        for (String pair : pairs) {
            int eq = pair.indexOf('=');
            if (eq < 1) throw new IllegalArgumentException("Bad SET pair: " + pair);

            String col = pair.substring(0, eq).trim();
            String lit = pair.substring(eq + 1).trim();

            Column schemaCol = t.getColumns().stream()
                    .filter(c -> c.getColumnType().equals(col))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Invalid column: " + col));

            newVals.put(col, literalToTyped(lit, schemaCol.getColumnType()));
        }

        int changed;
        if (where != null && !where.isBlank()) {
            Query<Row> q = parser.parse("SELECT * FROM " + tableName + " WHERE " + where + ";", db);
            changed = t.update(q.getFilter(), newVals);
        } else {
            changed = t.update(r -> true, newVals);
        }

        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = changed + " row(s) updated";
        resp.affectedRows = changed;
        return resp;
    }

    private QueryResponse handleDelete(Database db, String line) {
        Matcher m = RE_DELETE.matcher(line);
        if (!m.matches()) throw new IllegalArgumentException("Usage: DELETE FROM <name> [WHERE ...];");

        String tableName = m.group(1);
        String where     = m.group(2);

        Table t = db.getTable(tableName);

        int removed;
        if (where != null && !where.isBlank()) {
            Query<Row> q = parser.parse("SELECT * FROM " + tableName + " WHERE " + where + ";", db);
            removed = t.delete(q.getFilter());
        } else {
            removed = t.delete(r -> true);
        }

        QueryResponse resp = new QueryResponse();
        resp.kind = "UPDATE_COUNT";
        resp.message = removed + " row(s) deleted";
        resp.affectedRows = removed;
        return resp;
    }


    private static Object literalToTyped(String literal, DataType type) {
        if (literal == null) return null;

        String lit = literal.trim();
        if (lit.equalsIgnoreCase("NULL")) return null;

        if (lit.length() >= 2 && lit.startsWith("'") && lit.endsWith("'")) {
            String body = lit.substring(1, lit.length() - 1).replace("''", "'");
            return switch (type) {
                case STRING -> body;
                case INTEGER -> Integer.parseInt(body);
                case DOUBLE -> Double.parseDouble(body);
                case BOOLEAN -> Boolean.parseBoolean(body);
            };
        }

        return switch (type) {
            case STRING -> lit;
            case INTEGER -> Integer.parseInt(lit);
            case DOUBLE -> Double.parseDouble(lit);
            case BOOLEAN -> Boolean.parseBoolean(lit);
        };
    }

    private static Object castNonString(String s, DataType t) {
        return switch (t) {
            case INTEGER -> Integer.parseInt(s);
            case DOUBLE  -> Double.parseDouble(s);
            case BOOLEAN -> Boolean.parseBoolean(s);
            case STRING  -> s;
        };
    }

    private static String[] splitByCommaRespectQuotes(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean inStr = false;

        for (int i = 0 ; i < s.length() ; i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                if (inStr && i + 1 < s.length() && s.charAt(i + 1) == '\'') {
                    buf.append('\''); i++; continue;
                }
                inStr = !inStr;
                buf.append(c);
                continue;
            }
            if (c == ',' && !inStr) {
                out.add(buf.toString().trim());
                buf.setLength(0);
            } else {
                buf.append(c);
            }

        }

        if (buf.length() > 0) out.add(buf.toString().trim());
        return out.toArray(new String[0]);
    }
}
