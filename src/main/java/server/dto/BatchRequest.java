package server.dto;

import java.util.List;

public record BatchRequest (List<String> sql, Boolean stopOnError){}
