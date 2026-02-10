package client;

import com.fasterxml.jackson.databind.ObjectMapper;
import server.dto.QueryRequest;
import server.dto.QueryResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ApiClient {
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private String baseUrl;

    public ApiClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String baseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String createSession() {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/sessions"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            int code = resp.statusCode();
            String body = resp.body();

            if (code / 100 != 2) {
                throw new RuntimeException("HTTP " + code + " from /api/sessions: " + body);
            }

            var node = mapper.readTree(body);
            var sid = node.get("sessionId");
            if (sid == null || sid.isNull() || sid.asText().isBlank()) {
                throw new RuntimeException("No sessionId in response: " + body);
            }
            return sid.asText();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public QueryResponse query(String sessionId, String sql){
        try {
            String json = mapper.writeValueAsString(new QueryRequest(sql));

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/query?sessionId=" + sessionId))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            String body = http.send(req, HttpResponse.BodyHandlers.ofString()).body();
            return mapper.readValue(body, QueryResponse.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
