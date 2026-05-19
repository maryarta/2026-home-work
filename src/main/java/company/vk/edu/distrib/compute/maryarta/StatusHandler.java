package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

public class StatusHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
            exchange.sendResponseHeaders(200,0);
        } else {
            exchange.sendResponseHeaders(405,0);
        }
        exchange.close();
    }
}
