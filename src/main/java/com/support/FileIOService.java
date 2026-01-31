package com.support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Stream;

public class FileIOService {

  private final Path filteredDir;
  private final Path preProcessedFiles;

  public FileIOService() throws IOException {
    this.filteredDir = makeDir("filtered_files");
    this.preProcessedFiles = makeDir("pre_processed_files");
  }

  public Path getFilteredDir() {
    return filteredDir;
  }

  public Path getPreProcessedFiles() {
    return preProcessedFiles;
  }

  public void filterFile(List<Path> files, String field, String filter, String separator)
      throws IOException {

    String normalizedFilter = filter.replaceAll("\\s", "").toLowerCase();

    for (Path file : files) {
      Path outputFile = getPath(file, filteredDir);

      try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8);
          BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {

        Iterator<String> iterator = lines.iterator();
        if (!iterator.hasNext()) continue;

        String header = iterator.next();
        List<String> columns =
            Arrays.stream(header.split(separator))
                .map(col -> col.replace("\"", "").trim())
                .toList();

        int fieldIndex = columns.indexOf(field);
        if (fieldIndex == -1) {
          throw new IllegalArgumentException("Campo '" + field + "' não encontrado");
        }

        writer.write(header);
        writer.newLine();

        while (iterator.hasNext()) {
          String line = iterator.next();
          String[] values = line.split(separator);

          if (fieldIndex < values.length) {
            String value = values[fieldIndex].replace("\"", "").replaceAll("\\s", "").toLowerCase();

            if (value.equals(normalizedFilter)) {
              writer.write(line);
              writer.newLine();
            }
          }
        }
      }
    }
  }

  public void concatCsvFiles(List<Path> files) throws IOException {

    Path outputFile = preProcessedFiles.resolve("consolidated_quarters_by_description.csv");

    boolean isFirstFile = true;

    try (BufferedWriter writer =
        Files.newBufferedWriter(
            outputFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      for (Path file : files) {
        try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {

          Iterator<String> it = lines.iterator();
          if (!it.hasNext()) continue;

          String header = it.next();

          if (isFirstFile) {
            writer.write(header);
            writer.newLine();
            isFirstFile = false;
          }

          it.forEachRemaining(
              line -> {
                try {
                  writer.write(line);
                  writer.newLine();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
        }
      }
    }
  }

  public void removeDuplicates(Path inputFile, boolean maintainFirstOccurrence) throws IOException {

    Path outputFile = preProcessedFiles.resolve("unique_" + inputFile.getFileName());

    Set<String> processedLines = new LinkedHashSet<>();

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String line;
      boolean isFirstLine = true;

      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) continue;

        if (isFirstLine) {
          writer.write(line);
          writer.newLine();
          isFirstLine = false;
          continue;
        }

        if (maintainFirstOccurrence) {
          processedLines.add(line);
        } else {
          processedLines.remove(line);
          processedLines.add(line);
        }
      }

      for (String uniqueLine : processedLines) {
        writer.write(uniqueLine);
        writer.newLine();
      }
    }
  }

  private static Path getPath(Path file, Path baseDir) {
    return baseDir.resolve("filtered_" + file.getFileName());
  }

  private static Path makeDir(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }
}
