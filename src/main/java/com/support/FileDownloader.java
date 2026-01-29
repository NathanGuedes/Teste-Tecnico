package com.support;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FileDownloader {

    private final String baseUrl;
    private final List<String> urls;

    public FileDownloader(String baseUrl, List<String> urls) {
        this.baseUrl = baseUrl;
        this.urls = urls;
    }

    private Path makeDir() {
        Path dir = Path.of(System.getProperty("user.dir"), "compress");

        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao criar diretório", e);
        }

        return dir;
    }



    public void downloadFile() {
        Path folderPath = makeDir();

        HttpClient client = HttpClient.newHttpClient();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String url : urls) {
            String fullUrl = baseUrl + url;

            String fileName = Path.of(URI.create(fullUrl).getPath())
                    .getFileName()
                    .toString();

            Path destination = folderPath.resolve(fileName);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(fullUrl))
                    .GET()
                    .build();

            CompletableFuture<Void> future =
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofFile(destination))
                            .thenAccept(response -> {
                                if (response.statusCode() == 200) {
                                    System.out.println("Download Concluido localizado em: " + destination);
                                } else {
                                    System.out.println("Falha no download. Status: " + response.statusCode());
                                }
                            })
                            .exceptionally(ex -> {
                                System.out.println("Erro no download: " + ex.getMessage());
                                return null;
                            });

            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}

