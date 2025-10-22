package Query_Layer;

import core.*;

import java.util.*;
import java.util.function.Predicate;

public class QueryParser {

    private static final Set<String> OPS = Set.of("!=", "<>", ">=", "<=", "=", ">", "<", "LIKE");

    public Query<Row> parse(String sql, Database database) {
        if (sql == null) throw new QueryException("Query is null");

        sql = sql.trim();
        if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
        sql = sql.replaceAll("\\s+", " ");

        String upper = sql.toUpperCase(Locale.ROOT);
        if (!upper.startsWith("SELECT ")) {
            throw new QueryException("Query must start with SELECT");
        }

        int fromIdx   = upper.indexOf(" FROM ");
        if (fromIdx == -1) throw new QueryException("Missing FROM clause");

        int whereIdx  = upper.indexOf(" WHERE ");
        int orderIdx  = upper.indexOf(" ORDER BY ");
        int limitIdx  = upper.indexOf(" LIMIT ");

        String selectPart = sql.substring(7, fromIdx).trim();
        List<String> selectedColumns = parseColumns(selectPart);

        int fromBodyEnd = minPositive(whereIdx, orderIdx, limitIdx, sql.length());
        String tablePart = sql.substring(fromIdx + 6, fromBodyEnd).trim();
        String tableName = extractTableName(tablePart);

        Table table = database.getTable(tableName);
        if (table == null) throw new QueryException("Table not found: " + tableName);

        Predicate<Row> filter = row -> true;
        if (whereIdx != -1) {
            int whereBodyStart = whereIdx + " WHERE ".length();
            int whereBodyEnd = minPositive(orderIdx, limitIdx, -1, sql.length());
            String whereClause = sql.substring(whereBodyStart, whereBodyEnd).trim();
            if (whereClause.isEmpty()) throw new QueryException("Empty WHERE clause");
            filter = parseWhere(whereClause);
        }

        List<Query.OrderSpec> orderSpecs = Collections.emptyList();
        if (orderIdx != -1) {
            int orderBodyStart = orderIdx + " ORDER BY ".length();
            int orderBodyEnd   = (limitIdx != -1) ? limitIdx : sql.length();
            String orderClause = sql.substring(orderBodyStart, orderBodyEnd).trim();
            if (orderClause.isEmpty()) throw new QueryException("Empty ORDER BY clause");
            orderSpecs = parseOrderBy(orderClause);
        }

        Integer limit = null;
        Integer offset = null;
        if (limitIdx != -1) {
            String limitTail = sql.substring(limitIdx + " LIMIT ".length()).trim();
            LimitSpec ls = parseLimit(limitTail);
            limit = ls.limit;
            offset = ls.offset;
        }

        Query<Row> query = new Query<>();
        query.setTable(table);
        query.setSelectedColumns(selectedColumns);
        query.setFilter(filter);
        query.setMapper(r -> r);
        query.setLimit(limit);
        if (offset != null) query.setOffset(offset);
        if (!orderSpecs.isEmpty()) query.setOrderBy(orderSpecs);

        return query;
    }

    private int minPositive(int a, int b, int c, int fallback) {
        int res = Integer.MAX_VALUE;
        if (a != -1) res = Math.min(res, a);
        if (b != -1) res = Math.min(res, b);
        if (c != -1) res = Math.min(res, c);
        return (res == Integer.MAX_VALUE) ? fallback : res;
    }

    private List<String> parseColumns(String part) {
        if (part.equals("*")) return Collections.emptyList();
        String[] toks = part.split(",");
        List<String> cols = new ArrayList<>(toks.length);
        for (String t : toks) {
            String c = t.trim();
            if (!c.isEmpty()) cols.add(c);
        }
        return cols;
    }

    private String extractTableName(String tablePart) {
        int sp = tablePart.indexOf(' ');
        return (sp == -1) ? tablePart : tablePart.substring(0, sp).trim();
    }

    // ---------- WHERE ----------
    private java.util.function.Predicate<Row> parseWhere(String clause) {
        List<Object> parts = splitByLogical(clause);
        if (parts.isEmpty() || !(parts.get(0) instanceof String)) {
            throw new QueryException("Invalid WHERE clause: " + clause);
        }

        java.util.function.Predicate<Row> acc = parsePredicate((String) parts.get(0));
        for (int i = 1; i < parts.size(); i += 2) {
            String logic = ((String) parts.get(i)).toUpperCase(Locale.ROOT);
            java.util.function.Predicate<Row> right = parsePredicate((String) parts.get(i + 1));
            if ("AND".equals(logic)) acc = acc.and(right);
            else if ("OR".equals(logic)) acc = acc.or(right);
            else throw new QueryException("Unsupported logical operator: " + logic);
        }
        return acc;
    }

    private List<Object> splitByLogical(String clause) {
        List<Object> out = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean inStr = false;

        for (int i = 0; i < clause.length(); i++) {
            char c = clause.charAt(i);

            if (c == '\'') {
                if (inStr && i + 1 < clause.length() && clause.charAt(i + 1) == '\'') {
                    buf.append(c); i++; continue;
                }
                inStr = !inStr; buf.append(c); continue;
            }

            if (!inStr) {
                if (regionEqualsIgnoreCase(clause, i, " AND ")) {
                    out.add(buf.toString().trim());
                    out.add("AND");
                    buf.setLength(0);
                    i += 4; // " AND "
                    continue;
                } else if (regionEqualsIgnoreCase(clause, i, " OR ")) {
                    out.add(buf.toString().trim());
                    out.add("OR");
                    buf.setLength(0);
                    i += 3; // " OR "
                    continue;
                }
            }
            buf.append(c);
        }

        if (buf.length() > 0) out.add(buf.toString().trim());
        return out;
    }

    private boolean regionEqualsIgnoreCase(String s, int offset, String needle) {
        int n = needle.length();
        if (offset + n > s.length()) return false;
        return s.regionMatches(true, offset, needle, 0, n);
    }

    private java.util.function.Predicate<Row> parsePredicate(String expr) {
        String op = OPS.stream()
                .filter(o -> indexOfOp(expr, o) >= 0)
                .max(Comparator.comparingInt(String::length))
                .orElseThrow(() -> new QueryException("Unsupported operator in: " + expr));

        int idx = indexOfOp(expr, op);
        String column = expr.substring(0, idx).trim();
        String rawValue = expr.substring(idx + op.length()).trim();

        if (column.isEmpty() || rawValue.isEmpty()) {
            throw new QueryException("Invalid predicate: " + expr);
        }

        return row -> {
            Value<?> cell = row.getValue(column);
            if (cell == null) return false;
            Object value = parseValue(rawValue);
            return cell.compare(op, new Value<>(value, cell.getType()));
        };
    }

    private int indexOfOp(String s, String op) {
        boolean inStr = false;
        for (int i = 0; i <= s.length() - op.length(); i++) {
            char c = s.charAt(i);

            if (c == '\'') {
                if (inStr && i + 1 < s.length() && s.charAt(i + 1) == '\'') { i++; continue; }
                inStr = !inStr;
            }

            if (!inStr && s.regionMatches(true, i, op, 0, op.length())) return i;
        }
        return -1;
    }

    private List<Query.OrderSpec> parseOrderBy(String clause) {
        String[] parts = clause.split(",");
        List<Query.OrderSpec> specs = new ArrayList<>();
        for (String p : parts) {
            String s = p.trim();
            if (s.isEmpty()) continue;
            String[] toks = s.split("\\s+");
            String col = toks[0].trim();
            boolean asc = true;
            if (toks.length > 1) {
                String dir = toks[1].trim().toUpperCase(Locale.ROOT);
                if ("DESC".equals(dir)) asc = false;
                else if (!"ASC".equals(dir)) throw new QueryException("ORDER BY: expected ASC/DESC, got: " + toks[1]);
            }
            specs.add(new Query.OrderSpec(col, asc));
        }
        return specs;
    }

    private static class LimitSpec { Integer limit; Integer offset; }

    private LimitSpec parseLimit(String tail) {
        String s = tail.trim();
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1);
        s = s.trim();

        LimitSpec spec = new LimitSpec();
        String upper = s.toUpperCase(Locale.ROOT);

        int offIdx = upper.indexOf(" OFFSET ");
        if (offIdx >= 0) {
            String left = s.substring(0, offIdx).trim();
            String right = s.substring(offIdx + " OFFSET ".length()).trim();
            int lim = parseNonNegativeInt(left, "LIMIT");
            int off = parseNonNegativeInt(right, "OFFSET");
            spec.limit = lim; spec.offset = off; return spec;
        }

        if (s.contains(",")) {
            String[] p = s.split(",");
            if (p.length != 2) throw new QueryException("Invalid LIMIT format: " + tail);
            int off = parseNonNegativeInt(p[0].trim(), "LIMIT offset");
            int lim = parseNonNegativeInt(p[1].trim(), "LIMIT count");
            spec.limit = lim; spec.offset = off; return spec;
        }

        int lim = parseNonNegativeInt(s, "LIMIT");
        spec.limit = lim; return spec;
    }

    private int parseNonNegativeInt(String s, String label) {
        try {
            int v = Integer.parseInt(s);
            if (v < 0) throw new QueryException(label + " must be >= 0");
            return v;
        } catch (NumberFormatException e) {
            throw new QueryException("Invalid " + label + " value: " + s);
        }
    }
    private Object parseValue(String raw) {
        String unq = unquote(raw);

        if (unq.equalsIgnoreCase("true") || unq.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(unq);
        }
        if (unq.matches("[-+]?\\d+")) {
            try { return Integer.parseInt(unq); } catch (NumberFormatException ignored) {}
        }
        if (unq.matches("[-+]?\\d*\\.\\d+")) {
            try { return Double.parseDouble(unq); } catch (NumberFormatException ignored) {}
        }
        return unq;
    }

    private String unquote(String s) {
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            String body = s.substring(1, s.length() - 1);
            return body.replace("''", "'"); // '' -> '
        }
        return s;
    }
}
