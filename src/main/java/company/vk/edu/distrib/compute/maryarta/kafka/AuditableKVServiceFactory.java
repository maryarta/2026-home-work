package company.vk.edu.distrib.compute.maryarta.kafka;

import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class AuditableKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new AuditableKVServiceImpl(port);
    }
}
