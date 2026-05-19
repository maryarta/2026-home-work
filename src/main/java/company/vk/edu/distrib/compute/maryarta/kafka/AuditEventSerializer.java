package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AuditEventSerializer implements Serializer<AuditEvent> {
    @Override
    public byte[] serialize(String topic, AuditEvent event) {
        if (event == null) {
            return null;
        }
        try (
                ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                DataOutputStream dataStream = new DataOutputStream(byteStream)
        ) {
            dataStream.writeUTF(event.method());
            dataStream.writeUTF(event.id());
            dataStream.writeLong(event.timestamp());
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Failed to serialize AuditEvent", e);
        }
    }
}
