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

  // URL base onde os arquivos ZIP estão hospedados
  private final String baseUrl;

  // Caminhos relativos dos arquivos ZIP a serem processados
  private final List<String> archivePaths;

  public ZipArchiveService(String baseUrl, List<String> archivePaths) {
    this.baseUrl = baseUrl;
    this.archivePaths = archivePaths;
  }

  // Cria um diretório no diretório raiz do projeto
  private Path createDirectory(String directoryName) {
    Path dir = Path.of(System.getProperty("user.dir"), directoryName);

    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new RuntimeException("Erro ao criar diretório: " + directoryName, e);
    }

    return dir;
  }

  // Realiza o download assíncrono e extração dos arquivos ZIP
  public void downloadAndExtractArchives() {

    // Diretório para armazenar os arquivos ZIP baixados
    Path downloadDir = createDirectory("compress");

    // Diretório onde os arquivos serão extraídos
    Path extractDir = createDirectory("extract");

    // Cliente HTTP para realizar os downloads
    HttpClient client = HttpClient.newHttpClient();

    // Lista de futures para controle das execuções assíncronas
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (String archivePath : archivePaths) {

      // Monta a URL completa do arquivo
      String fullUrl = baseUrl + archivePath;

      // Extrai o nome do arquivo a partir da URL
      String fileName = Path.of(URI.create(fullUrl).getPath()).getFileName().toString();

      // Caminho final do arquivo ZIP
      Path destination = downloadDir.resolve(fileName);

      // Ignora o download se o arquivo já existir
      if (Files.exists(destination)) continue;

      // Cria a requisição HTTP
      HttpRequest request = HttpRequest.newBuilder().uri(URI.create(fullUrl)).GET().build();

      // Executa o download de forma assíncrona
      CompletableFuture<Void> future =
          client
              .sendAsync(request, HttpResponse.BodyHandlers.ofFile(destination))
              .thenAccept(
                  response -> {
                    // Verifica se o download foi bem-sucedido
                    if (response.statusCode() == 200) {
                      System.out.println("Download concluído: " + destination);
                      extractZip(destination, extractDir);
                    } else {
                      System.out.println("Falha no download. Status: " + response.statusCode());
                    }
                  })
              .exceptionally(
                  ex -> {
                    // Trata erro durante o download
                    System.out.println("Erro no download: " + ex.getMessage());
                    return null;
                  });

      futures.add(future);
    }

    // Aguarda a conclusão de todos os downloads
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  // Extrai o conteúdo de um arquivo ZIP para o diretório alvo
  private void extractZip(Path zipFile, Path targetDir) {

    System.out.println("Extraindo: " + zipFile.getFileName());

    try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(zipFile))) {

      ZipEntry entry;

      // Itera sobre as entradas do arquivo ZIP
      while ((entry = zip.getNextEntry()) != null) {

        // Normaliza o caminho do arquivo extraído
        Path newPath = targetDir.resolve(entry.getName()).normalize();

        // Cria diretórios ou arquivos conforme o tipo da entrada
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
