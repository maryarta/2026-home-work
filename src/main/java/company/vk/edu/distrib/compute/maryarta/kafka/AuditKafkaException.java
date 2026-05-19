package company.vk.edu.distrib.compute.maryarta.kafka;

public class AuditKafkaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public AuditKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
