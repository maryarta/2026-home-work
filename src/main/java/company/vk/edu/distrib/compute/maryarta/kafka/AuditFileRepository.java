package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class AuditFileRepository {
    private final Path filePath;

    public AuditFileRepository(String fileName) {
        this.filePath = Path.of(fileName);
    }

    public synchronized void save(AuditEvent event) {
        try {
            if (Files.notExists(filePath)) {
                Files.createFile(filePath);
            }
            String line = event.method() + ";" + event.id() + ";" + event.timestamp() + System.lineSeparator();
            Files.writeString(filePath, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save audit event to file", e);
        }
    }

    public synchronized List<AuditEvent> findAll() {
        try {
            if (Files.notExists(filePath)) {
                return List.of();
            }
            List<String> lines = Files.readAllLines(filePath, StandardCharsets.UTF_8);
            List<AuditEvent> events = new ArrayList<>();
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split(";", 3);
                if (parts.length != 3) {
                    continue;
                }
                String method = parts[0];
                String id = parts[1];
                long timestamp = Long.parseLong(parts[2]);
                events.add(new AuditEvent(method, id, timestamp));
            }
            return events;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read audit events from file", e);
        }

    }
}
