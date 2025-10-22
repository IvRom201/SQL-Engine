package Console_Layer;

import Query_Layer.*;
import core.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsoleApp {

    private static final Pattern RE_CREATE =
            Pattern.compile("^CREATE\\s+TABLE\\s+(\\w+)\\s*\\((.+)\\)\\s*;?$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern RE_ALTER_ADD =
            Pattern.compile("^ALTER\\s+TABLE\\s+(\\w+)\\s+ADD\\s+COLUMN\\s+(\\w+)\\s+(STRING|INTEGER|DOUBLE|BOOLEAN)(\\s+PRIMARY\\s+KEY)?\\s*;?$",
                    Pattern.CASE_INSENSITIVE);

    private static final Pattern RE_DROP =
            Pattern.compile("^DROP\\s+TABLE\\s+(\\w+)\\s*;?$", Pattern.CASE_INSENSITIVE);

    private static final Pattern RE_INSERT =
            Pattern.compile("^INSERT\\s+INTO\\s+(\\w+)\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern RE_UPDATE =
            Pattern.compile("^UPDATE\\s+(\\w+)\\s+SET\\s+(.+?)(?:\\s+WHERE\\s+(.+))?\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private static final Pattern RE_DELETE =
            Pattern.compile("^DELETE\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.+))?\\s*;?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public static void main(String[] args) throws Exception {
        Database db = new Database();
        QueryParser parser = new QueryParser();
        QueryExecutor executor = new QueryExecutor();

        System.out.println("MiniSQL console. Type HELP for commands.");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print("> ");
            String line = in.readLine();
            if (line == null) break;
            line = line.trim();
            if (line.isEmpty()) continue;

            if (!line.endsWith(";") &&
                    (startsWithIgnoreCase(line, "SELECT ") ||
                            startsWithIgnoreCase(line, "UPDATE ") ||
                            startsWithIgnoreCase(line, "INSERT ") )) {
                StringBuilder sb = new StringBuilder(line);
                while (!sb.toString().trim().endsWith(";")) {
                    System.out.print("... ");
                    String more = in.readLine();
                    if (more == null) break;
                    sb.append(' ').append(more);
                }
                line = sb.toString();
            }

            String upper = line.toUpperCase(Locale.ROOT);

            try {
                if (upper.equals("HELP")) {
                    printHelp();
                } else if (upper.equals("EXIT") || upper.equals("QUIT")) {
                    System.out.println("Bye!");
                    break;
                } else if (upper.equals("TABLES")) {
                    db.listTables().forEach(System.out::println);
                } else if (upper.startsWith("DESCRIBE ")) {
                    String table = line.substring(9).trim().replaceAll(";$", "");
                    describe(db, table);
                } else if (upper.startsWith("LOAD ")) {
                    handleLoad(db, line);
                } else if (upper.startsWith("JOIN ")) {
                    handleJoin(db, line);
                } else if (upper.startsWith("AGG ")) {
                    handleAgg(db, line);
                } else if (upper.startsWith("CREATE TABLE")) {
                    handleCreate(db, line);
                } else if (upper.startsWith("ALTER TABLE")) {
                    handleAlterAdd(db, line);
                } else if (upper.startsWith("DROP TABLE")) {
                    handleDrop(db, line);
                } else if (upper.startsWith("INSERT INTO")) {
                    handleInsert(db, line);
                } else if (upper.startsWith("UPDATE ")) {
                    handleUpdate(db, line);
                } else if (upper.startsWith("DELETE FROM")) {
                    handleDelete(db, line);
                } else if (upper.startsWith("SELECT ")) {
                    Query<Row> q = parser.parse(line, db);
                    List<Row> rows = executor.execute(q);
                    TablePrinter.print(q.getTable(), q.getSelectedColumns(), q.getLimit(), q.getOffset(), System.out);
                } else {
                    System.out.println("Unknown command. Type HELP.");
                }
            } catch (QueryException qe) {
                System.out.println("[Query error] " + qe.getMessage());
            } catch (Exception e) {
                System.out.println("[Error] " + e.getMessage());
                e.printStackTrace(System.out);
            }
        }
    }

    private static void printHelp() {
        System.out.println("""
                Commands:
                  HELP
                  EXIT | QUIT
                  TABLES
                  DESCRIBE <table>
                  LOAD <table> FROM '<path>'
                  LOAD <table> FROM '<path>' DELIM=','          // ',', ';' or '\\t'
                  -- DDL:
                  CREATE TABLE <name> (col TYPE [PRIMARY KEY], col2 TYPE, ...)
                  ALTER TABLE <name> ADD COLUMN <col> <TYPE> [PRIMARY KEY]
                  DROP TABLE <name>
                  -- DML:
                  INSERT INTO <name>(col, col2, ...) VALUES (v1, v2, ...)
                  UPDATE <name> SET col=val[, col2=val2 ...] [WHERE ...]
                  DELETE FROM <name> [WHERE ...]
                  -- Query:
                  SELECT <cols> FROM <table> [WHERE ...] [ORDER BY ...] [LIMIT n [OFFSET m] | LIMIT m, n];
                  -- Utilities:
                  JOIN <left> <right> ON leftCol=rightCol [INNER|LEFT|RIGHT]
                  AGG <table> <FUNC(col|*)> [WHERE ...]         // COUNT, MIN, MAX, SUM, AVG
                """);
    }

    private static void handleCreate(Database db, String line) {
        Matcher m = RE_CREATE.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: CREATE TABLE <name> (col TYPE [PRIMARY KEY], ...);");
            return;
        }
        String tableName = m.group(1);
        String colsSpec = m.group(2).trim();
        String[] defs = splitByCommaRespectQuotes(colsSpec);
        List<Column> cols = new ArrayList<>();
        for (String def : defs) {
            String[] toks = def.trim().split("\\s+");
            if (toks.length < 2) {
                System.out.println("Bad column def: " + def);
                return;
            }
            String colName = toks[0];
            DataType type = parseType(toks[1]);
            Column c = new Column(colName, type);
            if (toks.length >= 3) {
                String rest = String.join(" ", Arrays.copyOfRange(toks, 2, toks.length)).toUpperCase(Locale.ROOT);
                if (rest.contains("PRIMARY KEY")) c.setPrimaryKey(true);
            }
            cols.add(c);
        }

        db.createTable(tableName, cols);
        System.out.println("Table created: " + tableName);
    }

    private static void handleAlterAdd(Database db, String line) {
        Matcher m = RE_ALTER_ADD.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: ALTER TABLE <name> ADD COLUMN <col> <TYPE> [PRIMARY KEY];");
            return;
        }
        String tableName = m.group(1);
        String col = m.group(2);
        DataType type = DataType.valueOf(m.group(3).toUpperCase(Locale.ROOT));
        boolean pk = m.group(4) != null;

        Table t = db.getTable(tableName);
        Column c = new Column(col, type);
        c.setPrimaryKey(pk);
        t.addColumn(c);
        System.out.printf("Table %s: column %s %s added%s%n",
                tableName, col, type, pk ? " (PK)" : "");
    }

    private static void handleDrop(Database db, String line) {
        Matcher m = RE_DROP.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: DROP TABLE <name>;");
            return;
        }
        String tableName = m.group(1);
        db.dropTable(tableName);
        System.out.println("Dropped table: " + tableName);
    }

    private static void handleInsert(Database db, String line) {
        Matcher m = RE_INSERT.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: INSERT INTO <name>(c1, c2, ...) VALUES (v1, v2, ...);");
            return;
        }
        String tableName = m.group(1);
        String cols = m.group(2);
        String vals = m.group(3);

        String[] colNames = splitByCommaRespectQuotes(cols);
        String[] valLits = splitByCommaRespectQuotes(vals);
        if (colNames.length != valLits.length) {
            System.out.println("Columns count != values count");
            return;
        }

        Table t = db.getTable(tableName);
        Map<String, Object> toInsert = new LinkedHashMap<>();
        for (int i = 0; i < colNames.length; i++) {
            String col = colNames[i].trim();
            String lit = valLits[i].trim();
            Column schemaCol = t.getColumns().stream()
                    .filter(c -> c.getColumnName().equals(col))
                    .findFirst().orElseThrow(() -> new RuntimeException("Unknown column: " + col));
            toInsert.put(col, literalToTyped(lit, schemaCol.getColumnType()));
        }
        t.insert(toInsert);
        System.out.println("1 row inserted into " + tableName);
    }

    private static void handleUpdate(Database db, String line) {
        Matcher m = RE_UPDATE.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: UPDATE <name> SET col=val[, col2=val2 ...] [WHERE ...];");
            return;
        }
        String tableName = m.group(1);
        String setSpec   = m.group(2).trim();
        String where     = m.group(3);

        Table t = db.getTable(tableName);

        Map<String, Object> newVals = new LinkedHashMap<>();
        String[] pairs = splitByCommaRespectQuotes(setSpec);
        for (String p : pairs) {
            int eq = p.indexOf('=');
            if (eq < 1) throw new RuntimeException("Bad SET pair: " + p);
            String col = p.substring(0, eq).trim();
            String lit = p.substring(eq + 1).trim();
            Column schemaCol = t.getColumns().stream()
                    .filter(c -> c.getColumnName().equals(col))
                    .findFirst().orElseThrow(() -> new RuntimeException("Unknown column: " + col));
            newVals.put(col, literalToTyped(lit, schemaCol.getColumnType()));
        }

        int changed;
        if (where != null && !where.isBlank()) {
            QueryParser qp = new QueryParser();
            Query<Row> q = qp.parse("SELECT * FROM " + tableName + " WHERE " + where + ";", db);
            changed = t.update(q.getFilter(), newVals);
        } else {
            changed = t.update(r -> true, newVals);
        }
        System.out.println(changed + " row(s) updated");
    }

    private static void handleDelete(Database db, String line) {
        Matcher m = RE_DELETE.matcher(line);
        if (!m.matches()) {
            System.out.println("Usage: DELETE FROM <name> [WHERE ...];");
            return;
        }
        String tableName = m.group(1);
        String where = m.group(2);

        Table t = db.getTable(tableName);

        int removed;
        if (where != null && !where.isBlank()) {
            QueryParser qp = new QueryParser();
            Query<Row> q = qp.parse("SELECT * FROM " + tableName + " WHERE " + where + ";", db);
            removed = t.delete(q.getFilter());
        } else {
            removed = t.delete(r -> true);
        }
        System.out.println(removed + " row(s) deleted");
    }

    private static void describe(Database db, String tableName) {
        try {
            Table t = db.getTable(tableName);
            System.out.println("Table: " + t.getTableName());
            System.out.println("Columns:");
            t.getColumns().forEach(c ->
                    System.out.printf("  %s : %s%s%n",
                            c.getColumnName(), c.getColumnType(), c.isPrimaryKey() ? " (PK)" : "")
            );
            System.out.println("Rows: " + t.getRows().size());
        } catch (Exception e) {
            System.out.println("[Error] " + e.getMessage());
        }
    }

    private static void handleLoad(Database db, String line) throws Exception {
        String up = line.toUpperCase(Locale.ROOT);
        int fromIdx = up.indexOf(" FROM ");
        if (fromIdx < 0) throw new IllegalArgumentException("Usage: LOAD <table> FROM '<path>' [DELIM=',' ]");

        String table = line.substring("LOAD ".length(), fromIdx).trim();

        String tail = line.substring(fromIdx + " FROM ".length()).trim();
        if (!tail.startsWith("'")) throw new IllegalArgumentException("Path must be quoted with single quotes");
        int endQuote = tail.indexOf("'", 1);
        if (endQuote < 0) throw new IllegalArgumentException("Unterminated path string");
        String pathStr = tail.substring(1, endQuote);
        String after = tail.substring(endQuote + 1).trim();

        Character delim = null;
        if (!after.isEmpty()) {
            String upAfter = after.toUpperCase(Locale.ROOT);
            if (upAfter.startsWith("DELIM=")) {
                String d = after.substring(6).trim();
                if (d.equals("'\\t'") || d.equals("\"\\t\"") || d.equals("\\t")) delim = '\t';
                else if (d.startsWith("'") && d.endsWith("'") && d.length() == 3) delim = d.charAt(1);
                else if (d.startsWith("\"") && d.endsWith("\"") && d.length() == 3) delim = d.charAt(1);
                else if (d.length() == 1) delim = d.charAt(0);
                else throw new IllegalArgumentException("DELIM must be one char: ',', ';' or '\\t'");
            } else {
                throw new IllegalArgumentException("Unexpected tail after path: " + after);
            }
        }

        CsvLoader.load(db, table, Path.of(pathStr));
        System.out.printf("Loaded table '%s' from %s%n", table, pathStr);
    }

    private static void handleJoin(Database db, String line) {
        String[] toks = line.split("\\s+");
        if (toks.length < 6 || !"ON".equalsIgnoreCase(toks[3])) {
            System.out.println("Usage: JOIN <left> <right> ON <leftCol>=<rightCol> [INNER|LEFT|RIGHT]");
            return;
        }
        String leftName = toks[1];
        String rightName = toks[2];

        String onExpr = toks[4];
        int eq = onExpr.indexOf('=');
        if (eq < 1 || eq == onExpr.length() - 1) {
            System.out.println("ON must be like leftCol=rightCol");
            return;
        }
        String leftCol = onExpr.substring(0, eq);
        String rightCol = onExpr.substring(eq + 1);

        JoinType jt = JoinType.INNER;
        if (toks.length >= 6) {
            String mode = toks[toks.length - 1].toUpperCase(Locale.ROOT);
            if (mode.equals("LEFT")) jt = JoinType.LEFT;
            else if (mode.equals("RIGHT")) jt = JoinType.RIGHT;
            else if (mode.equals("INNER")) jt = JoinType.INNER;
        }

        Table left = db.getTable(leftName);
        Table right = db.getTable(rightName);

        var result = Join.join(left, right, jt, Join.eq(leftCol, rightCol));

        System.out.printf("JOIN %s %s ON %s=%s %s -> %d rows%n",
                leftName, rightName, leftCol, rightCol, jt, result.size());

        int i = 1;
        for (var pair : result) {
            System.out.printf("[%d]%n", i++);
            if (pair.left != null) System.out.println("  LEFT : " + pair.left.getValues());
            else System.out.println("  LEFT : null");
            if (pair.right != null) System.out.println("  RIGHT: " + pair.right.getValues());
            else System.out.println("  RIGHT: null");
        }
    }

    private static void handleAgg(Database db, String line) {
        String up = line.toUpperCase(Locale.ROOT);
        String rest = line.substring(4).trim();

        String whereClause = null;
        int whereIdx = up.indexOf(" WHERE ");
        if (whereIdx >= 0) {
            whereClause = line.substring(whereIdx + " WHERE ".length()).trim();
            rest = line.substring(4, whereIdx).trim();
        }

        String[] toks = rest.split("\\s+");
        if (toks.length < 2) {
            System.out.println("Usage: AGG <table> <FUNC(col|*)> [WHERE ...]");
            return;
        }
        String tableName = toks[0];
        String funcCall = toks[1];

        int lp = funcCall.indexOf('(');
        int rp = funcCall.lastIndexOf(')');
        if (lp < 1 || rp < 0 || rp <= lp) {
            System.out.println("FUNC must be like SUM(col), COUNT(*), MIN(col) etc.");
            return;
        }
        String func = funcCall.substring(0, lp).toUpperCase(Locale.ROOT);
        String arg = funcCall.substring(lp + 1, rp).trim(); // "col" или "*"

        Table table = db.getTable(tableName);

        List<Row> rows;
        if (whereClause != null && !whereClause.isBlank()) {
            QueryParser p = new QueryParser();
            Query<Row> q = p.parse("SELECT * FROM " + tableName + " WHERE " + whereClause + ";", db);
            rows = new QueryExecutor().execute(q);
        } else {
            rows = table.getRows();
        }

        Object result;
        switch (func) {
            case "COUNT" -> {
                if ("*".equals(arg)) result = Aggregator.count(rows);
                else result = Aggregator.countNotNull(rows, arg);
            }
            case "MIN" -> result = Aggregator.min(rows, arg);
            case "MAX" -> result = Aggregator.max(rows, arg);
            case "SUM" -> result = Aggregator.sum(rows, arg);
            case "AVG" -> result = Aggregator.avg(rows, arg);
            default -> {
                System.out.println("Unknown aggregate: " + func + " (use COUNT, MIN, MAX, SUM, AVG)");
                return;
            }
        }

        System.out.printf("%s(%s) = %s%n", func, arg, result);
    }

    private static boolean startsWithIgnoreCase(String s, String prefix) {
        return s.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private static DataType parseType(String tok) {
        return DataType.valueOf(tok.toUpperCase(Locale.ROOT));
    }

    private static Object literalToTyped(String literal, DataType type) {
        String lit = literal.trim();
        if (lit.startsWith("'") && lit.endsWith("'") && lit.length() >= 2) {
            String body = lit.substring(1, lit.length() - 1).replace("''", "'");
            return type == DataType.STRING ? body : castNonString(body, type);
        }
        if (type == DataType.BOOLEAN) {
            return Boolean.parseBoolean(lit);
        }
        return switch (type) {
            case INTEGER -> Integer.parseInt(lit);
            case DOUBLE -> Double.parseDouble(lit);
            case STRING -> lit;
            case BOOLEAN -> Boolean.parseBoolean(lit);
        };
    }

    private static Object castNonString(String s, DataType t) {
        return switch (t) {
            case INTEGER -> Integer.parseInt(s);
            case DOUBLE -> Double.parseDouble(s);
            case BOOLEAN -> Boolean.parseBoolean(s);
            case STRING -> s;
        };
    }

    private static String[] splitByCommaRespectQuotes(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean inStr = false;
        for (int i = 0; i < s.length(); i++) {
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
