package Query_Layer;

import core.Row;
import core.Table;
import core.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class QueryExecutor {

    public <T> List<T> execute(Query<T> q) {
        Table table = q.getTable();
        if (table == null) throw new QueryException("No table to execute");
        List<Row> rows = new ArrayList<>(table.getRows());

        if (q.getFilter() != null) {
            rows = rows.stream().filter(q.getFilter()).collect(Collectors.toList());
        }

        if (q.hasOrder()) {
            Comparator<Row> cmp = null;
            for (Query.OrderSpec spec : q.getOrderBy()) {
                Comparator<Row> c = comparatorFor(spec.getColumn(), spec.isAsc());
                cmp = (cmp == null) ? c : cmp.thenComparing(c);
            }
            if (cmp != null) rows.sort(cmp);
        }

        int from = q.hasOffset() ? q.getOffset() : 0;
        int to = rows.size();
        if (q.hasLimit()) {
            to = Math.min(rows.size(), from + q.getLimit());
        }
        if (from > rows.size()) {
            rows = List.of();
        } else {
            rows = rows.subList(from, to);
        }

        return rows.stream().map(q.getMapper()).collect(Collectors.toList());
    }

    private Comparator<Row> comparatorFor(String column, boolean asc) {
        Comparator<Row> c = (r1, r2) -> compareValues(r1.getValue(column), r2.getValue(column));
        return asc ? c : c.reversed();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareValues(Value<?> v1, Value<?> v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return 1;
        if (v2 == null) return -1;

        if (v1.compare("=", (Value) v2)) return 0;
        if (v1.compare(">", (Value) v2)) return 1;
        if (v1.compare("<", (Value) v2)) return -1;

        return 0;
    }
}
