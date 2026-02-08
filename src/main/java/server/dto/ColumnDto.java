package server.dto;

public class ColumnDto {
    public String name;
    public String type;

    public ColumnDto(String name, String type) {
        this.name = name;
        this.type = type;
    }
    public ColumnDto() {}
}
