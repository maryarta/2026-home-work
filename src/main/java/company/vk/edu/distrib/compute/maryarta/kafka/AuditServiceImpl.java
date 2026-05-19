package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class AuditServiceImpl implements AuditService {
    private final KafkaConsumer<String, AuditEvent> consumer;
    private boolean running = true;
    private Thread thread;
    private final AuditFileRepository auditFileRepository;

    public AuditServiceImpl(String bootstrapServers, String consumerGroupId) {
        consumer = createConsumer(bootstrapServers, consumerGroupId);
        auditFileRepository = new AuditFileRepository("audit" + consumerGroupId + "-" + UUID.randomUUID() + ".log");
    }

    @Override
    public void start() {
        if (thread != null && thread.isAlive()) {
            return;
        }
        running = true;
        thread = new Thread(this::consumeAuditEvents);
        thread.start();
    }

    private void consumeAuditEvents() {
        try (KafkaConsumer<String, AuditEvent> kafkaConsumer = consumer) {
            kafkaConsumer.subscribe(List.of("audit"));
            while (running) {
                ConsumerRecords<String, AuditEvent> records =
                        kafkaConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, AuditEvent> record : records) {
                    auditFileRepository.save(record.value());
                }
                if (!records.isEmpty()) {
                    kafkaConsumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumer.close();
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return auditFileRepository.findAll();
    }

    private KafkaConsumer<String, AuditEvent> createConsumer(String bootstrapServers, String consumerGroupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AuditEventDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }
}
