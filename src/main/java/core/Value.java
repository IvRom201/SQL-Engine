package core;

import java.util.Objects;
import java.util.regex.Pattern;

public class Value<T> {
    private T data;
    private final DataType type;

    public Value(T data, DataType type) {
        this.data = data;
        this.type = type;
    }

    public void setData(T newValue) {
        this.data = newValue;
    }

    public T get() {
        return data;
    }

    public T getRaw() {
        return data;
    }

    public DataType getType() {
        return type;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public boolean compare(String operator, Value<?> other) {
        if (other == null) {
            return switch (operator) {
                case "=" -> this.data == null;
                case "!=","<>" -> this.data != null;
                default -> false;
            };
        }

        DataType t1 = this.getType();
        DataType t2 = other.getType();

        if (isNumeric(t1) && isNumeric(t2)) {
            double a = asDouble(this.data);
            double b = asDouble(other.get());
            return compareNumbers(a, b, operator);
        }

        if (t1 != t2) {
            throw new IllegalArgumentException("Incompatible types in compare: " + t1 + " vs " + t2);
        }

        return switch (t1) {
            case INTEGER, DOUBLE -> compareNumbers(asDouble(this.data), asDouble(other.get()), operator);
            case STRING -> compareStrings((String) this.data, (String) other.get(), operator);
            case BOOLEAN -> compareBooleans((Boolean) this.data, (Boolean) other.get(), operator);
        };
    }

    private static boolean isNumeric(DataType t) {
        return t == DataType.INTEGER || t == DataType.DOUBLE;
    }

    private static double asDouble(Object o) {
        if (o == null) return Double.NaN;
        if (o instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(String.valueOf(o)); } catch (Exception e) { return Double.NaN; }
    }

    private boolean compareNumbers(double a, double b, String operator) {
        return switch (operator) {
            case "=" -> Double.compare(a, b) == 0;
            case "!=", "<>" -> Double.compare(a, b) != 0;
            case ">" -> Double.compare(a, b) > 0;
            case ">=" -> Double.compare(a, b) >= 0;
            case "<" -> Double.compare(a, b) < 0;
            case "<=" -> Double.compare(a, b) <= 0;
            default -> throw new IllegalArgumentException("Invalid operator for numeric comparison: " + operator);
        };
    }

    private boolean compareStrings(String a, String b, String operator) {
        if (a == null || b == null) {
            return switch (operator) {
                case "=" -> Objects.equals(a, b);
                case "!=", "<>" -> !Objects.equals(a, b);
                default -> false;
            };
        }
        return switch (operator) {
            case "=" -> a.equals(b);
            case "!=", "<>" -> !a.equals(b);
            case ">" -> a.compareTo(b) > 0;
            case ">=" -> a.compareTo(b) >= 0;
            case "<" -> a.compareTo(b) < 0;
            case "<=" -> a.compareTo(b) <= 0;
            case "LIKE" -> like(a, b); // поддержка % и _
            default -> throw new IllegalArgumentException("Invalid operator for string comparison: " + operator);
        };
    }

    private static boolean like(String text, String pattern) {
        String regex = Pattern.quote(pattern)
                .replace("%", "\\E.*\\Q")
                .replace("_", "\\E.\\Q");
        return Pattern.compile("^" + regex + "$").matcher(text).find();
    }

    private boolean compareBooleans(Boolean a, Boolean b, String operator) {
        return switch (operator) {
            case "=" -> Objects.equals(a, b);
            case "!=", "<>" -> !Objects.equals(a, b);
            default -> throw new IllegalArgumentException("Invalid operator for boolean comparison: " + operator);
        };
    }

    @Override
    public String toString() {
        return String.valueOf(data);
    }
}
