package com.support;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipArchiveService {

  private final String baseUrl;
  private final List<String> archivePaths;

  public ZipArchiveService(String baseUrl, List<String> archivePaths) {
    this.baseUrl = baseUrl;
    this.archivePaths = archivePaths;
  }

  private Path createDirectory(String directoryName) {
    Path dir = Path.of(System.getProperty("user.dir"), directoryName);

    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new RuntimeException("Erro ao criar diretório: " + directoryName, e);
    }

    return dir;
  }

  public void downloadAndExtractArchives() {
    Path downloadDir = createDirectory("compress");
    Path extractDir = createDirectory("extract");

    HttpClient client = HttpClient.newHttpClient();
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (String archivePath : archivePaths) {
      String fullUrl = baseUrl + archivePath;
      String fileName = Path.of(URI.create(fullUrl).getPath()).getFileName().toString();

      Path destination = downloadDir.resolve(fileName);
      if (Files.exists(destination)) continue;

      HttpRequest request = HttpRequest.newBuilder().uri(URI.create(fullUrl)).GET().build();

      CompletableFuture<Void> future =
          client
              .sendAsync(request, HttpResponse.BodyHandlers.ofFile(destination))
              .thenAccept(
                  response -> {
                    if (response.statusCode() == 200) {
                      System.out.println("Download concluído: " + destination);
                      extractZip(destination, extractDir);
                    } else {
                      System.out.println("Falha no download. Status: " + response.statusCode());
                    }
                  })
              .exceptionally(
                  ex -> {
                    System.out.println("Erro no download: " + ex.getMessage());
                    return null;
                  });

      futures.add(future);
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  private void extractZip(Path zipFile, Path targetDir) {
    System.out.println("Extraindo: " + zipFile.getFileName());

    try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(zipFile))) {
      ZipEntry entry;

      while ((entry = zip.getNextEntry()) != null) {
        Path newPath = targetDir.resolve(entry.getName()).normalize();

        if (entry.isDirectory()) {
          Files.createDirectories(newPath);
        } else {
          Files.createDirectories(newPath.getParent());
          Files.copy(zip, newPath, StandardCopyOption.REPLACE_EXISTING);
        }

        zip.closeEntry();
      }

    } catch (IOException e) {
      throw new RuntimeException("Erro ao extrair arquivo: " + zipFile, e);
    }
  }
}
