package Console_Layer;

import core.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class CsvLoader {

    public static void load(Database db, String tableName, Path csvPath) throws IOException {
        try (BufferedReader br = Files.newBufferedReader(csvPath)) {
            load(db, tableName, br, null);
        }
    }

    public static void load(Database db, String tableName, Reader reader, Character delimiterOpt) throws IOException {
        BufferedReader br = reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);

        br.mark(1 << 20);
        String headerLine = br.readLine();
        if (headerLine == null) throw new IOException("CSV is empty");
        char delim = delimiterOpt != null ? delimiterOpt : detectDelimiter(headerLine);
        List<String> headers = parseCsvLine(headerLine, delim);

        List<List<String>> rows = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            if (line.isEmpty()) { rows.add(Collections.emptyList()); continue; }
            List<String> fields = parseCsvLine(line, delim);
            while (fields.size() < headers.size()) fields.add(null);
            if (fields.size() > headers.size()) fields = fields.subList(0, headers.size());
            rows.add(fields);
        }
        br.reset();

        List<DataType> types = inferTypes(headers.size(), rows);

        List<Column> cols = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            String name = sanitizeHeader(headers.get(i), i);
            cols.add(new Column(name, types.get(i)));
        }
        db.createTable(tableName, cols);

        Table table = db.getTable(tableName);
        for (List<String> r : rows) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                String h = cols.get(i).getColumnName();
                Object v = cast(r.size() > i ? r.get(i) : null, types.get(i));
                map.put(h, v);
            }
            table.insert(map);
        }
    }

    private static String sanitizeHeader(String raw, int index) {
        if (raw == null || raw.isBlank()) return "col_" + (index + 1);
        String s = raw.trim().replaceAll("\\s+", "_");
        if (!Character.isLetter(s.charAt(0))) s = "c_" + s;
        return s;
    }

    private static List<DataType> inferTypes(int colCount, List<List<String>> rows) {
        List<DataType> types = new ArrayList<>(Collections.nCopies(colCount, DataType.INTEGER));
        for (List<String> r : rows) {
            for (int i = 0; i < colCount; i++) {
                String val = (i < r.size() ? r.get(i) : null);
                types.set(i, widen(types.get(i), classify(val)));
            }
        }
        return types;
    }

    private static DataType classify(String s) {
        if (s == null || s.isBlank()) return DataType.STRING;
        String t = s.trim();
        if (t.equalsIgnoreCase("true") || t.equalsIgnoreCase("false")) return DataType.BOOLEAN;
        if (t.matches("[-+]?\\d+")) return DataType.INTEGER;
        if (t.matches("[-+]?\\d*\\.\\d+")) return DataType.DOUBLE;
        return DataType.STRING;
    }

    private static DataType widen(DataType current, DataType seen) {
        if (current == seen) return current;
        // BOOLEAN < INTEGER < DOUBLE < STRING
        if (current == DataType.STRING || seen == DataType.STRING) return DataType.STRING;
        if (current == DataType.DOUBLE || seen == DataType.DOUBLE) return DataType.DOUBLE;
        if (current == DataType.INTEGER || seen == DataType.INTEGER) return DataType.INTEGER;
        return DataType.BOOLEAN;
    }

    private static Object cast(String s, DataType t) {
        if (s == null) return null;
        String v = s.trim();
        if (v.isEmpty()) return null;
        try {
            return switch (t) {
                case INTEGER -> Integer.parseInt(v);
                case DOUBLE -> Double.parseDouble(v);
                case BOOLEAN -> Boolean.parseBoolean(v);
                case STRING -> v;
            };
        } catch (Exception e) {
            return t == DataType.STRING ? v : null;
        }
    }

    private static char detectDelimiter(String headerLine) {
        int commas = count(headerLine, ',');
        int semis  = count(headerLine, ';');
        int tabs   = count(headerLine, '\t');
        if (tabs >= commas && tabs >= semis) return '\t';
        if (semis >= commas) return ';';
        return ',';
    }

    private static int count(String s, char ch) {
        int c = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == ch) c++;
        return c;
    }

    private static List<String> parseCsvLine(String line, char delimiter) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    cur.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == delimiter && !inQuotes) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        out.add(cur.toString());
        return out;
    }
}
