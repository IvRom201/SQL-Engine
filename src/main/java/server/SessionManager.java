package server;

import core.Database;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SessionManager {
    public static final class SessionState {
        public final Database db = new Database();
    }

    private final ConcurrentHashMap<String, SessionState> sessions = new ConcurrentHashMap<>();

    public String createSession() {
        String id = UUID.randomUUID().toString();
        sessions.put(id, new SessionState());
        return id;
    }

    public SessionState getOrThrow(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            throw new IllegalArgumentException("sessionId is required");
        }
        SessionState st = sessions.get(sessionId);
        if (st == null) throw new NotFoundException("Session not found: " + sessionId);
        return st;
    }

    public void delete(String sessionId) {
        sessions.remove(sessionId);
    }
}
