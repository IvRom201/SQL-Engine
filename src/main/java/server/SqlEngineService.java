package server;

import core.Database;
import core.Table;
import org.springframework.stereotype.Service;
import server.dto.BatchRequest;
import server.dto.BatchResponse;
import server.dto.QueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class SqlEngineService {
    private final Database db = new Database();
    private final SqlDispatcher dispatcher = new SqlDispatcher();

    public synchronized QueryResponse execute(String sql) {
        return dispatcher.execute(db, sql);
    }

    public Set<String> listTables() {
        return db.listTables();
    }

    public Table getTableOrThrow(String name) {
        try {
            return db.getTable(name);
        } catch (RuntimeException e) {
            throw new NotFoundException(e.getMessage());
        }
    }

    public synchronized BatchResponse executeBatch(BatchRequest req) {
        boolean stopOnError = req.stopOnError() == null || req.stopOnError();

        List<QueryResponse> results = new ArrayList<>();
        int executed = 0;
        int failed = 0;

        for (String s : req.sql()) {
            try {
                results.add(dispatcher.execute(db, s));
                executed++;
            } catch (Exception e) {
                failed++;
                results.add(QueryResponse.error(e.getMessage()));
                if (stopOnError) break;
            }
        }

        return new BatchResponse(executed, failed, results);
    }
}
