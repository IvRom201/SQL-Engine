package Query_Layer;

import core.Database;
import core.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;



public class QueryBuilder {

    private final Database database;
    private final QueryParser parser = new QueryParser();

    private final List<String> selectCols = new ArrayList<>();
    private String tableName;
    private final List<String> whereParts = new ArrayList<>();
    private final List<Query.OrderSpec> orderSpecs = new ArrayList<>();
    private Integer limit;
    private Integer offset;

    public QueryBuilder(Database database) { this.database = database; }

    public QueryBuilder select(String... cols) {
        if (cols != null) for (String c : cols) if (c != null && !c.isBlank()) selectCols.add(c.trim());
        return this;
    }

    public QueryBuilder selectAll() { selectCols.clear(); return this; } // эквивалент "*"

    public QueryBuilder from(String table) { this.tableName = table; return this; }

    public QueryBuilder where(String rawCondition) {
        if (rawCondition != null && !rawCondition.isBlank()) {
            if (!whereParts.isEmpty()) whereParts.add("AND " + rawCondition.trim());
            else whereParts.add(rawCondition.trim());
        }
        return this;
    }

    public QueryBuilder and(String rawCondition) {
        if (rawCondition != null && !rawCondition.isBlank()) {
            whereParts.add("AND " + rawCondition.trim());
        }
        return this;
    }

    public QueryBuilder or(String rawCondition) {
        if (rawCondition != null && !rawCondition.isBlank()) {
            whereParts.add("OR " + rawCondition.trim());
        }
        return this;
    }

    public QueryBuilder orderBy(String column, boolean asc) {
        orderSpecs.add(new Query.OrderSpec(column, asc));
        return this;
    }

    public QueryBuilder limit(int n) { this.limit = n; return this; }
    public QueryBuilder offset(int n) { this.offset = n; return this; }

    public Query<Row> build() {
        if (tableName == null || tableName.isBlank()) {
            throw new QueryException("FROM table is required");
        }
        String sql = buildSql();
        return parser.parse(sql, database);
    }

    public <T> Query<T> build(Function<Row, T> mapper) {
        Query<Row> base = build();
        Query<T> q = new Query<>();
        q.setTable(base.getTable());
        q.setSelectedColumns(base.getSelectedColumns());
        q.setFilter(base.getFilter());
        q.setLimit(base.getLimit());
        q.setOffset(base.getOffset());
        q.setOrderBy(base.getOrderBy());
        q.setMapper(mapper);
        return q;
    }

    private String buildSql() {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        if (selectCols.isEmpty()) sb.append("*");
        else {
            StringJoiner sj = new StringJoiner(", ");
            selectCols.forEach(sj::add);
            sb.append(sj);
        }

        sb.append(" FROM ").append(tableName);

        if (!whereParts.isEmpty()) {
            sb.append(" WHERE ");
            for (int i = 0; i < whereParts.size(); i++) {
                if (i > 0) sb.append(' ');
                sb.append(whereParts.get(i));
            }
        }

        if (!orderSpecs.isEmpty()) {
            sb.append(" ORDER BY ");
            StringJoiner sj = new StringJoiner(", ");
            for (Query.OrderSpec s : orderSpecs) {
                sj.add(s.getColumn() + " " + (s.isAsc() ? "ASC" : "DESC"));
            }
            sb.append(sj);
        }

        if (limit != null) {
            sb.append(" LIMIT ").append(limit);
            if (offset != null && offset > 0) {
                sb.append(" OFFSET ").append(offset);
            }
        }

        return sb.toString();
    }
}
