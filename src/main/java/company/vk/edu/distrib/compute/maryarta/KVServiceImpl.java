package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new FileDao();
        createContext();
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void createContext() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao, null));
    }

}
