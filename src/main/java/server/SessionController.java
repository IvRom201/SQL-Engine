package server;

import org.springframework.web.bind.annotation.*;
import server.dto.CreateSessionResponse;

@RestController
@RequestMapping
public class SessionController {
    private final SessionManager sessions;

    public SessionController(SessionManager sessions) {
        this.sessions = sessions;
    }

    @PostMapping("/sessions")
    public CreateSessionResponse create(){
        return new CreateSessionResponse(sessions.createSession());
    }

    @DeleteMapping("/sessions/{id}")
    public void delete(@PathVariable String id){
        sessions.delete(id);
    }
}
