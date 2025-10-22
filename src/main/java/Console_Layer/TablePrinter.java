package Console_Layer;

import core.Row;
import core.Table;
import core.Value;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class TablePrinter {

    public static void print(Table table, List<String> selectedColumns, Integer limit, Integer offset, PrintStream out) {
        if (table == null) {
            out.println("(no table)");
            return;
        }
        List<String> cols = new ArrayList<>();
        if (selectedColumns == null || selectedColumns.isEmpty()) {
            table.getColumns().forEach(c -> cols.add(c.getColumnName()));
        } else {
            cols.addAll(selectedColumns);
        }

        int total = table.getRows().size();
        int from = (offset != null && offset > 0) ? offset : 0;
        int to = (limit != null && limit >= 0) ? Math.min(total, from + limit) : total;
        if (from > total) {
            out.printf("Rows %d–%d of %d%n", 0, 0, total);
            out.println("(empty)");
            return;
        }

        List<Row> page = table.getRows().subList(from, to);

        int[] widths = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) widths[i] = cols.get(i).length();
        for (Row r : page) {
            for (int i = 0; i < cols.size(); i++) {
                Value<?> v = r.getValue(cols.get(i));
                String s = v == null ? "NULL" : String.valueOf(v.getRaw());
                widths[i] = Math.max(widths[i], s.length());
            }
        }

        printSeparator(widths, out);
        printRow(cols, widths, out);
        printSeparator(widths, out);

        for (Row r : page) {
            List<String> vals = new ArrayList<>(cols.size());
            for (String c : cols) {
                Value<?> v = r.getValue(c);
                vals.add(v == null ? "NULL" : String.valueOf(v.getRaw()));
            }
            printRow(vals, widths, out);
        }
        printSeparator(widths, out);

        out.printf("Rows %d–%d of %d%n", from + 1, to, total);
    }

    private static void printSeparator(int[] widths, PrintStream out) {
        out.print('+');
        for (int w : widths) {
            out.print("-".repeat(w + 2));
            out.print('+');
        }
        out.println();
    }

    private static void printRow(List<String> cells, int[] widths, PrintStream out) {
        out.print('|');
        for (int i = 0; i < cells.size(); i++) {
            String s = cells.get(i);
            out.print(' ');
            out.print(padRight(s, widths[i]));
            out.print(' ');
            out.print('|');
        }
        out.println();
    }

    private static String padRight(String s, int w) {
        if (s.length() >= w) return s;
        return s + " ".repeat(w - s.length());
    }
}
