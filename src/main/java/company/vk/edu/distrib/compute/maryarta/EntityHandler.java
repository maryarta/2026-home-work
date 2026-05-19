package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.maryarta.kafka.AuditSender;

import java.io.IOException;
import java.util.NoSuchElementException;

public class EntityHandler implements HttpHandler {
    private final Dao<byte[]> dao;
    AuditSender auditSender;

    public EntityHandler(Dao<byte[]> dao, AuditSender auditSender) {
        this.dao = dao;
        this.auditSender = auditSender;
    }
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        try {
            String id = parseId(query);
            long timestamp = System.currentTimeMillis();

            if (auditSender != null) {
                auditSender.send(method, id, timestamp);
            }

            switch (method) {
                case "GET" -> {
                    byte [] value = dao.get(id);
                    exchange.sendResponseHeaders(200, value.length); // OK
                    exchange.getResponseBody().write(value);
                }
                case "PUT" -> {
                    byte[] newValue = exchange.getRequestBody().readAllBytes();
                    dao.upsert(id, newValue);
                    exchange.sendResponseHeaders(201, 0); // OK
                }
                case "DELETE" -> {
                    dao.delete(id);
                    exchange.sendResponseHeaders(202, 0);
                }
                default -> exchange.sendResponseHeaders(405, 0);
            }
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(400, 0); // Bad Request
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, 0); // Not Found
        }
        exchange.close();
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }
}
