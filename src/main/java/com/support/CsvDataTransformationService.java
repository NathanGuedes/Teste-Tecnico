package com.support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CsvDataTransformationService {

  private final Path validatedFilesDir;
  private final Path mergedFilesDir;

  public CsvDataTransformationService() throws IOException {
    this.validatedFilesDir = createDirectory("validated_files");
    this.mergedFilesDir = createDirectory("merged_files");
  }

  public Path getValidatedFilesDir() {
    return validatedFilesDir;
  }

  public Path getMergedFilesDir() {
    return mergedFilesDir;
  }

  private static Map<String, Integer> readCsvHeaderIndex(Path csvFile, String delimiter)
      throws IOException {

    try (Stream<String> lines = Files.lines(csvFile, StandardCharsets.UTF_8)) {

      String header =
          lines.findFirst().orElseThrow(() -> new IllegalArgumentException("Arquivo CSV vazio"));

      String[] columns = header.split(delimiter);

      Map<String, Integer> headerIndex = new LinkedHashMap<>();

      for (int i = 0; i < columns.length; i++) {
        String column = columns[i].replace("\"", "").trim();

        headerIndex.put(column, i);
      }

      return headerIndex;
    }
  }

  public Path validateColumnByRegex(Path inputFile, String columnName, String regex, String delimiter) throws IOException {

    // Lê o índice do cabeçalho (nome da coluna -> posição)
    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    // Valida se a coluna existe
    Integer columnIndex = headerIndex.get(columnName);
    if (columnIndex == null) {
      throw new IllegalArgumentException("Coluna '" + columnName + "' não encontrada no CSV");
    }

    // Prepara regex
    Pattern pattern = Pattern.compile(regex);

    // Arquivo de saída
    Path outputFile = validatedFilesDir.resolve("validated_" + inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      // Lê o cabeçalho original
      String headerLine = reader.readLine();

      // Escreve cabeçalho + nova coluna
      writer.write(headerLine + delimiter + "valid_" + columnName);
      writer.newLine();

      String line;

      // Processa as linhas do arquivo
      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        String valueToValidate = columnIndex < values.length ? values[columnIndex] : "";

        // Remove aspas e espaços antes da validação
        valueToValidate = valueToValidate.replace("\"", "").trim();

        boolean matches = pattern.matcher(valueToValidate).matches();

        // Escreve linha original + flag de validação
        writer.write(line + delimiter + (matches ? "1" : "0"));
        writer.newLine();
      }
    }

    return outputFile;
  }

  private static Path createDirectory(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }
}
