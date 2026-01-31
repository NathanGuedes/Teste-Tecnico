package com.support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

public class CsvFileProcessor {

  private final Path filteredOutputDir;
  private final Path preProcessedOutputDir;

  public CsvFileProcessor() throws IOException {
    this.filteredOutputDir = createDirectory("filtered_files");
    this.preProcessedOutputDir = createDirectory("pre_processed_files");
  }

  public Path getFilteredOutputDir() {
    return filteredOutputDir;
  }

  public Path getPreProcessedOutputDir() {
    return preProcessedOutputDir;
  }

  public void filterCsvFilesByColumnValue(
      List<Path> inputFiles, String columnName, String expectedValue, String delimiter)
      throws IOException {

    String normalizedExpectedValue = expectedValue.replaceAll("\\s", "").toLowerCase();

    for (Path inputFile : inputFiles) {
      Path outputFile = resolveFilteredOutputPath(inputFile);

      try (Stream<String> lines = Files.lines(inputFile, StandardCharsets.UTF_8);
          BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {

        Iterator<String> iterator = lines.iterator();
        if (!iterator.hasNext()) continue;

        String header = iterator.next();
        List<String> columns =
            Arrays.stream(header.split(delimiter))
                .map(col -> col.replace("\"", "").trim())
                .toList();

        int columnIndex = columns.indexOf(columnName);
        if (columnIndex == -1) {
          throw new IllegalArgumentException("Coluna '" + columnName + "' não encontrada");
        }

        writer.write(header);
        writer.newLine();

        while (iterator.hasNext()) {
          String line = iterator.next();
          String[] values = line.split(delimiter);

          if (columnIndex < values.length) {
            String value =
                values[columnIndex].replace("\"", "").replaceAll("\\s", "").toLowerCase();

            if (value.equals(normalizedExpectedValue)) {
              writer.write(line);
              writer.newLine();
            }
          }
        }
      }
    }
  }

  public void mergeCsvFiles(List<Path> files) throws IOException {

    Path outputFile = preProcessedOutputDir.resolve("consolidated_quarters_by_description.csv");

    boolean writeHeader = true;

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

          if (writeHeader) {
            writer.write(header);
            writer.newLine();
            writeHeader = false;
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

  public void removeDuplicateLines(Path inputFile, boolean keepFirstOccurrence) throws IOException {

    Path outputFile = preProcessedOutputDir.resolve("unique_" + inputFile.getFileName());

    Set<String> uniqueLines = new LinkedHashSet<>();

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String line;
      boolean isHeader = true;

      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) continue;

        if (isHeader) {
          writer.write(line);
          writer.newLine();
          isHeader = false;
          continue;
        }

        if (keepFirstOccurrence) {
          uniqueLines.add(line);
        } else {
          uniqueLines.remove(line);
          uniqueLines.add(line);
        }
      }

      for (String uniqueLine : uniqueLines) {
        writer.write(uniqueLine);
        writer.newLine();
      }
    }
  }

  private Path resolveFilteredOutputPath(Path inputFile) {
    return filteredOutputDir.resolve("filtered_" + inputFile.getFileName());
  }

  private static Path createDirectory(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }
}
