package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class AuditEventDeserializer implements Deserializer<AuditEvent> {
    @Override
    public AuditEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (
                ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                DataInputStream dataStream = new DataInputStream(byteStream)
        ) {
            String method = dataStream.readUTF();
            String id = dataStream.readUTF();
            long timestamp = dataStream.readLong();
            return new AuditEvent(method, id, timestamp);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize AuditEvent", e);
        }
    }
}
