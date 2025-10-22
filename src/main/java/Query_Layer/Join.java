package Query_Layer;

import core.Row;
import core.Table;
import core.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class Join {

    public static class RowPair {
        public final Row left;
        public final Row right;
        public RowPair(Row left, Row right) { this.left = left; this.right = right; }
    }

    public static List<RowPair> join(Table left, Table right, JoinType type, BiPredicate<Row, Row> on) {
        if (left == null || right == null) throw new IllegalArgumentException("Tables must not be null");
        if (on == null) throw new IllegalArgumentException("Join predicate must not be null");

        List<RowPair> out = new ArrayList<>();
        List<Row> L = left.getRows();
        List<Row> R = right.getRows();

        switch (type) {
            case INNER -> {
                for (Row l : L) {
                    for (Row r : R) {
                        if (on.test(l, r)) out.add(new RowPair(l, r));
                    }
                }
            }
            case LEFT -> {
                for (Row l : L) {
                    boolean matched = false;
                    for (Row r : R) {
                        if (on.test(l, r)) { out.add(new RowPair(l, r)); matched = true; }
                    }
                    if (!matched) out.add(new RowPair(l, null));
                }
            }
            case RIGHT -> {
                for (Row r : R) {
                    boolean matched = false;
                    for (Row l : L) {
                        if (on.test(l, r)) { out.add(new RowPair(l, r)); matched = true; }
                    }
                    if (!matched) out.add(new RowPair(null, r));
                }
            }
        }
        return out;
    }

    public static BiPredicate<Row, Row> eq(String leftCol, String rightCol) {
        return (l, r) -> {
            if (l == null || r == null) return false;
            Value<?> lv = l.getValue(leftCol);
            Value<?> rv = r.getValue(rightCol);
            if (lv == null || rv == null) return false;
            return lv.compare("=", rv);
        };
    }
}
