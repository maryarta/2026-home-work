package company.vk.edu.distrib.compute.maryarta.kafka;

@FunctionalInterface
public interface AuditSender {
    void send(String method, String id, long timestamp);
}
