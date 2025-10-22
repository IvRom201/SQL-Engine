package Query_Layer;

import core.Row;
import core.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Query<T> {
    private Table table;
    private List<String> selectedColumns = new ArrayList<>();
    private Predicate<Row> filter = r -> true;
    private Function<Row, T> mapper = r -> (T) r;

    private Integer limit;
    private Integer offset;
    private List<OrderSpec> orderBy = new ArrayList<>();

    public static class OrderSpec {
        private final String column;
        private final boolean asc;
        public OrderSpec(String column, boolean asc) { this.column = column; this.asc = asc; }
        public String getColumn() { return column; }
        public boolean isAsc() { return asc; }
    }

    public Table getTable() { return table; }
    public void setTable(Table table) { this.table = table; }

    public List<String> getSelectedColumns() { return selectedColumns; }
    public void setSelectedColumns(List<String> selectedColumns) { this.selectedColumns = selectedColumns; }

    public Predicate<Row> getFilter() { return filter; }
    public void setFilter(Predicate<Row> filter) { this.filter = filter; }

    public Function<Row, T> getMapper() { return mapper; }
    public void setMapper(Function<Row, T> mapper) { this.mapper = mapper; }

    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }
    public boolean hasLimit() { return limit != null && limit >= 0; }

    public Integer getOffset() { return offset; }
    public void setOffset(Integer offset) { this.offset = offset; }
    public boolean hasOffset() { return offset != null && offset > 0; }

    public List<OrderSpec> getOrderBy() { return orderBy; }
    public void setOrderBy(List<OrderSpec> orderBy) { this.orderBy = orderBy; }
    public boolean hasOrder() { return orderBy != null && !orderBy.isEmpty(); }
}
