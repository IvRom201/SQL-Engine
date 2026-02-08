package server;

import core.Database;
import org.springframework.stereotype.Service;
import server.dto.QueryResponse;

@Service
public class SqlEngineService {
    private final Database db = new Database();
    private final SqlDispatcher dispatcher = new SqlDispatcher();

    public synchronized QueryResponse execute(String sql) {
        return dispatcher.execute(db, sql);
    }
}
