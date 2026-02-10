package client;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import server.dto.QueryResponse;

import java.util.concurrent.CompletableFuture;

public class SqlClientApp extends Application {

    private final ApiClient api = new ApiClient("http://localhost:8080");
    private String sessionId;

    private final Label status = new Label("Disconnected");
    private final TextArea sqlArea = new TextArea("SELECT 1;");
    private final TextArea output = new TextArea();

    @Override
    public void start(Stage stage) {
        output.setEditable(false);

        TextField baseUrl = new TextField(api.baseUrl());

        Button connectButton = new Button("Connect");
        connectButton.setOnAction(e -> {
            api.setBaseUrl(baseUrl.getText().trim());
            status.setText("Connecting...");
            CompletableFuture.supplyAsync(api::createSession)
                    .thenAccept(id -> Platform.runLater(() -> {
                        sessionId = id;
                        status.setText("Connected. sessionId=" + id);
                    }))
                    .exceptionally(ex -> {
                        Platform.runLater(() -> status.setText("Error: " + ex.getMessage()));
                        return null;
                    });
        });

        Button runButton = new Button("Run");
        runButton.setOnAction(e -> {
            if (sessionId == null) {
                status.setText("Create session first (Connect).");
                return;
            }
            String sql = sqlArea.getText();
            status.setText("Running...");
            CompletableFuture.supplyAsync(() -> api.query(sessionId, sql))
                    .thenAccept(resp -> Platform.runLater(() -> render(resp)))
                    .exceptionally(ex -> {
                        Platform.runLater(() -> status.setText("Error: " + ex.getMessage()));
                        return null;
                    });
        });

        HBox top = new HBox(8, new Label("Server:"), baseUrl, connectButton, runButton, status);

        SplitPane center = new SplitPane(
                new VBox(new Label("SQL"), sqlArea),
                new VBox(new Label("Output"), output)
        );
        center.setDividerPositions(0.5);

        BorderPane root = new BorderPane(center);
        root.setTop(top);

        stage.setTitle("MiniDB Client");
        stage.setScene(new Scene(root, 1000, 600));
        stage.show();
    }

    private void render(QueryResponse resp) {
        status.setText(resp.kind() + " : " + resp.message());
        output.clear();

        if ("RESULT_SET".equals(resp.kind())) {
            output.appendText("Columns: " + resp.columns() + "\n");
            for (var row : resp.rows()) {
                output.appendText(row.toString());
                output.appendText("\n");
            }
        } else {
            output.appendText(resp.message() + "\n");
            if (resp.affectedRows() != null) {
                output.appendText("affectedRows=" + resp.affectedRows());
            }
        }
    }
}