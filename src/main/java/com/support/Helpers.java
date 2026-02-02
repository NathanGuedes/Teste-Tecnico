package com.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Helpers {
  public static void deleteDirectoryFromProjectRoot(String folderName) throws IOException {

    Path dir = Path.of(System.getProperty("user.dir"), folderName);

    if (!Files.exists(dir)) return;

    Files.walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
                throw new RuntimeException("Erro ao apagar: " + path, e);
              }
            });
  }

  public static void zipFiles(Path sourcePath) throws IOException {

    if (!Files.exists(sourcePath)) {
      throw new IllegalArgumentException("Caminho não existe: " + sourcePath);
    }

    String baseName = sourcePath.getFileName().toString().replace(".csv", "");

    Path zipPath = sourcePath.getParent().resolve(baseName + ".zip");

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {

      if (Files.isDirectory(sourcePath)) {

        Files.walk(sourcePath)
            .filter(Files::isRegularFile)
            .forEach(
                path -> {
                  try {
                    Path relative = sourcePath.relativize(path);
                    ZipEntry entry = new ZipEntry(sourcePath.getFileName() + "/" + relative);

                    zos.putNextEntry(entry);
                    Files.copy(path, zos);
                    zos.closeEntry();

                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

      } else {
        zos.putNextEntry(new ZipEntry(sourcePath.getFileName().toString()));
        Files.copy(sourcePath, zos);
        zos.closeEntry();
      }
    }
  }
}
