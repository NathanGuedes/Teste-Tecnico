package com.client;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ApiClient {

    private HttpClient client;

    public ApiClient() {
        this.client = HttpClient.newHttpClient();
    }

    public String requestHtml(String url) throws URISyntaxException, IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .GET()
                .build();

        HttpResponse<String> httpResponse = this.client.send(request, HttpResponse.BodyHandlers.ofString());

        return httpResponse.body();
    }
}
