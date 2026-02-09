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
    private final SessionManager sessions;
    private final SqlDispatcher dispatcher = new SqlDispatcher();

    public SqlEngineService(SessionManager sessions) {
        this.sessions = sessions;
    }

    public QueryResponse execute(String sessionId, String sql) {
        SessionManager.SessionState st = sessions.getOrThrow(sessionId);
        synchronized (st) {
            return dispatcher.execute(st.db, sql);
        }
    }

    public BatchResponse executeBatch(String sessionId, BatchRequest req) {
        SessionManager.SessionState st = sessions.getOrThrow(sessionId);
        synchronized (st) {
            boolean stopOnError = req.stopOnError() == null || req.stopOnError();

            var results = new ArrayList<QueryResponse>();
            int executed = 0;
            int failed = 0;

            for (String s : req.sql()) {
                try {
                    results.add(dispatcher.execute(st.db, s));
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

    public Set<String> listTables(String sessionId) {
        SessionManager.SessionState st = sessions.getOrThrow(sessionId);
        synchronized (st) {
            return st.db.listTables();
        }
    }

    public Table getTableOrThrow(String sessionId, String tableName) {
        SessionManager.SessionState st = sessions.getOrThrow(sessionId);
        synchronized (st) {
            return st.db.getTable(tableName);
        }
    }
}
