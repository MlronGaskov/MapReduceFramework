package ru.nsu.mr.endpoints;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public class CorsFilter extends Filter {

    private static final String ALLOWED_ORIGIN = "*";

    @Override
    public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
        Headers h = exchange.getResponseHeaders();
        h.add("Access-Control-Allow-Origin",  ALLOWED_ORIGIN);
        h.add("Access-Control-Allow-Headers", "Content-Type");
        h.add("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");

        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        chain.doFilter(exchange);
    }

    @Override
    public String description() {
        return "Adds CORS headers";
    }
}