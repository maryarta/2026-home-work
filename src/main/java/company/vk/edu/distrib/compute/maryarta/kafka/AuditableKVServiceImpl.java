package company.vk.edu.distrib.compute.maryarta.kafka;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.maryarta.EntityHandler;
import company.vk.edu.distrib.compute.maryarta.H2Dao;
import company.vk.edu.distrib.compute.maryarta.StatusHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class AuditableKVServiceImpl implements AuditableKVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;
    private KafkaProducer<String, AuditEvent> producer;
    private boolean async = true;

    public AuditableKVServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new H2Dao("data");
        createContext();
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        if (producer != null) {
            producer.close();
        }
        this.producer = createProducer(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        async = enabled;
    }

    @Override
    public void start() {
        if (producer == null) {
            throw new IllegalStateException("Kafka producer is not initialized");
        }
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        if (producer != null) {
            producer.close();
        }
    }

    private void createContext() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao, this::sendAuditEvent));
    }

    private void sendAuditEvent(String method, String id, long timestamp) {
        if (producer == null) {
            throw new IllegalStateException("Kafka producer is not initialized");
        }
        AuditEvent event = new AuditEvent(method, id, timestamp);
        ProducerRecord<String, AuditEvent> eventRecord = new ProducerRecord<>("audit", id, event);
        if (async) {
            producer.send(eventRecord);
        } else {
            try {
                producer.send(eventRecord).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Audit sending was interrupted", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to send audit event", e);
            }
        }
    }

    private static KafkaProducer<String, AuditEvent> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AuditEventSerializer.class);
        return new KafkaProducer<>(props);
    }
}
