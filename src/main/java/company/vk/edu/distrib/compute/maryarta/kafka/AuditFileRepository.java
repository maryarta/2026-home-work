package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditEvent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class AuditFileRepository {
    private final Path filePath;
    private static final int AUDIT_EVENT_FIELDS_COUNT = 3;
    private final ReentrantLock lock = new ReentrantLock();

    public AuditFileRepository(String fileName) {
        this.filePath = Path.of(fileName);
    }

    public void save(AuditEvent event) {
        lock.lock();
        try {
            if (Files.notExists(filePath)) {
                Files.createFile(filePath);
            }
            String line = event.method() + ";" + event.id() + ";" + event.timestamp() + System.lineSeparator();
            Files.writeString(filePath, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new AuditKafkaException("Failed to save audit event to file", e);
        } finally {
            lock.unlock();
        }
    }

    public List<AuditEvent> findAll() {
        lock.lock();
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
                String[] parts = line.split(";", AUDIT_EVENT_FIELDS_COUNT);
                if (parts.length != AUDIT_EVENT_FIELDS_COUNT) {
                    continue;
                }
                String method = parts[0];
                String id = parts[1];
                long timestamp = Long.parseLong(parts[2]);
                events.add(new AuditEvent(method, id, timestamp));
            }
            return events;
        } catch (IOException e) {
            throw new AuditKafkaException("Failed to read audit events from file", e);
        } finally {
            lock.unlock();
        }

    }
}
