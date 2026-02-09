package server.dto;

import java.util.List;

public record BatchResponse(int executed, int failed, List<QueryResponse> results) {}

