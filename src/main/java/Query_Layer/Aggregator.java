package Query_Layer;

import core.Row;
import core.Value;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

public class Aggregator {

    public static long count(List<Row> rows) {
        return rows == null ? 0 : rows.size();
    }

    public static long countNotNull(List<Row> rows, String column) {
        if (rows == null) return 0;
        return rows.stream().map(r -> r.getValue(column)).filter(Objects::nonNull).count();
    }

    public static Object min(List<Row> rows, String column) {
        if (rows == null || rows.isEmpty()) return null;
        return rows.stream()
                .map(r -> r.getValue(column))
                .filter(Objects::nonNull)
                .min((v1, v2) -> compare(v1, v2))
                .map(Value::get)
                .orElse(null);
    }

    public static Object max(List<Row> rows, String column) {
        if (rows == null || rows.isEmpty()) return null;
        return rows.stream()
                .map(r -> r.getValue(column))
                .filter(Objects::nonNull)
                .max((v1, v2) -> compare(v1, v2))
                .map(Value::get)
                .orElse(null);
    }

    public static BigDecimal sum(List<Row> rows, String column) {
        if (rows == null || rows.isEmpty()) return BigDecimal.ZERO;
        BigDecimal acc = BigDecimal.ZERO;
        for (Row r : rows) {
            Value<?> v = r.getValue(column);
            if (v == null) continue;
            BigDecimal bd = toBigDecimal(v.get());
            acc = acc.add(bd);
        }
        return acc;
    }

    public static BigDecimal avg(List<Row> rows, String column) {
        if (rows == null || rows.isEmpty()) return BigDecimal.ZERO;
        BigDecimal s = sum(rows, column);
        long n = rows.stream().map(r -> r.getValue(column)).filter(Objects::nonNull).count();
        return n == 0 ? BigDecimal.ZERO : s.divide(BigDecimal.valueOf(n), java.math.RoundingMode.HALF_UP);
    }

    private static int compare(Value<?> v1, Value<?> v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return 1;
        if (v2 == null) return -1;
        if (v1.compare("=", (Value<?>) v2)) return 0;
        if (v1.compare(">", (Value<?>) v2)) return 1;
        if (v1.compare("<", (Value<?>) v2)) return -1;
        return 0;
    }

    private static BigDecimal toBigDecimal(Object o) {
        if (o == null) return BigDecimal.ZERO;
        if (o instanceof BigDecimal bd) return bd;
        if (o instanceof Integer i) return BigDecimal.valueOf(i);
        if (o instanceof Long l) return BigDecimal.valueOf(l);
        if (o instanceof Double d) return BigDecimal.valueOf(d);
        if (o instanceof Float f) return BigDecimal.valueOf(f);
        try { return new BigDecimal(o.toString()); } catch (Exception e) { return BigDecimal.ZERO; }
    }
}
