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

  // Diretório onde serão salvos os arquivos filtrados
  private final Path filteredOutputDir;

  // Diretório onde serão salvos os arquivos pré-processados
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

  // Filtra múltiplos arquivos CSV com base no valor de uma coluna específica
  public void filterCsvFilesByColumnValue(
      List<Path> inputFiles, String columnName, String expectedValue, String delimiter)
      throws IOException {

    // Normaliza o valor esperado para comparação
    String normalizedExpectedValue = expectedValue.replaceAll("\\s", "").toLowerCase();

    for (Path inputFile : inputFiles) {

      // Define o arquivo de saída com prefixo filtered_
      Path outputFile = resolveFilteredOutputPath(inputFile);

      try (Stream<String> lines = Files.lines(inputFile, StandardCharsets.UTF_8);
          BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {

        Iterator<String> iterator = lines.iterator();
        if (!iterator.hasNext()) continue;

        // Lê o cabeçalho do CSV
        String header = iterator.next();

        // Extrai os nomes das colunas
        List<String> columns =
            Arrays.stream(header.split(delimiter))
                .map(col -> col.replace("\"", "").trim())
                .toList();

        // Obtém o índice da coluna desejada
        int columnIndex = columns.indexOf(columnName);
        if (columnIndex == -1) {
          throw new IllegalArgumentException("Coluna '" + columnName + "' não encontrada");
        }

        // Escreve o cabeçalho no arquivo de saída
        writer.write(header);
        writer.newLine();

        // Processa as linhas do arquivo
        while (iterator.hasNext()) {
          String line = iterator.next();
          String[] values = line.split(delimiter);

          if (columnIndex < values.length) {

            // Normaliza o valor da coluna para comparação
            String value =
                values[columnIndex].replace("\"", "").replaceAll("\\s", "").toLowerCase();

            // Escreve a linha caso o valor corresponda ao esperado
            if (value.equals(normalizedExpectedValue)) {
              writer.write(line);
              writer.newLine();
            }
          }
        }
      }
    }
  }

  // Consolida múltiplos arquivos CSV em um único arquivo mantendo apenas um cabeçalho
  public void mergeCsvFiles(List<Path> files) throws IOException {

    // Define o arquivo de saída consolidado
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

          // Lê o cabeçalho do arquivo atual
          String header = it.next();

          // Escreve o cabeçalho apenas uma vez
          if (writeHeader) {
            writer.write(header);
            writer.newLine();
            writeHeader = false;
          }

          // Escreve as demais linhas do arquivo
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

  // Remove linhas duplicadas de um CSV mantendo a primeira ou a última ocorrência
  public void removeDuplicateLines(Path inputFile, boolean keepFirstOccurrence) throws IOException {

    // Define o arquivo de saída com prefixo unique_
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

        // Escreve o cabeçalho sem aplicar remoção de duplicidade
        if (isHeader) {
          writer.write(line);
          writer.newLine();
          isHeader = false;
          continue;
        }

        // Controla a lógica de manutenção da primeira ou última ocorrência
        if (keepFirstOccurrence) {
          uniqueLines.add(line);
        } else {
          uniqueLines.remove(line);
          uniqueLines.add(line);
        }
      }

      // Escreve as linhas únicas no arquivo de saída
      for (String uniqueLine : uniqueLines) {
        writer.write(uniqueLine);
        writer.newLine();
      }
    }
  }

  // Resolve o caminho do arquivo filtrado com base no arquivo de entrada
  private Path resolveFilteredOutputPath(Path inputFile) {
    return filteredOutputDir.resolve("filtered_" + inputFile.getFileName());
  }

  // Cria um diretório na raiz do projeto caso não exista
  private static Path createDirectory(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }
}
