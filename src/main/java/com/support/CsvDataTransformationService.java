package com.support;

import com.support.enums.MathOperation;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class CsvDataTransformationService {

  private final Path validatedFilesDir;
  private final Path mergedFilesDir;
  private final Path calculetedFilesDir;

  public CsvDataTransformationService() throws IOException {
    this.validatedFilesDir = createDirectory("validated_files");
    this.mergedFilesDir = createDirectory("merged_files");
    this.calculetedFilesDir = createDirectory("calculated_files");
  }

  public Path getValidatedFilesDir() {
    return validatedFilesDir;
  }

  public Path getMergedFilesDir() {
    return mergedFilesDir;
  }

  public Path getCalculetedFilesDir() {
    return calculetedFilesDir;
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

  public void validateColumnByRegex(
      Path inputFile, String columnName, String regex, String delimiter) throws IOException {

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
  }

  public void calculateNewColumn(
      Path inputFile,
      String column1,
      String column2,
      String newColumnName,
      MathOperation operation,
      String delimiter)
      throws IOException {

    // Lê o índice do cabeçalho
    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    Integer index1 = headerIndex.get(column1);
    Integer index2 = headerIndex.get(column2);

    if (index1 == null || index2 == null) {
      throw new IllegalArgumentException("Colunas informadas não existem no CSV");
    }

    // Arquivo de saída
    Path outputFile = calculetedFilesDir.resolve("calculated_" + inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      // Cabeçalho original
      String headerLine = reader.readLine();

      // Escreve cabeçalho com nova coluna
      writer.write(headerLine + delimiter + '"' + newColumnName + '"');
      writer.newLine();

      String line;

      // Processa linhas
      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        double value1 = parseNumber(values, index1);
        double value2 = parseNumber(values, index2);

        double result = applyOperation(value1, value2, operation);

        String resultFormatado = String.format("%.2f", result);

        // Escreve linha original + resultado
        writer.write(
            line
                + delimiter
                + '"'
                + (delimiter.equals(";") ? resultFormatado.replace(".", ",") : resultFormatado)
                + '"');
        writer.newLine();
      }
    }
  }

  public void mergeCsvByKey(
      Path leftFile,
      Path rightFile,
      String leftKeyColumn,
      String rightKeyColumn,
      String delimiter)
      throws IOException {

    // ===== 1. Lê e indexa o arquivo da direita (B) =====
    Map<String, String[]> rightFileIndex = new HashMap<>();

    List<String> rightHeaders;
    int rightKeyIndex;

    try (BufferedReader reader = Files.newBufferedReader(rightFile, StandardCharsets.UTF_8)) {

      String headerLine = reader.readLine();
      if (headerLine == null) {
        throw new IllegalArgumentException("Arquivo da direita está vazio");
      }

      rightHeaders =
          Arrays.stream(headerLine.split(delimiter)).map(h -> h.replace("\"", "").trim()).toList();

      rightKeyIndex = rightHeaders.indexOf(rightKeyColumn);
      if (rightKeyIndex == -1) {
        throw new IllegalArgumentException(
            "Chave '" + rightKeyColumn + "' não encontrada no arquivo da direita");
      }

      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(delimiter, -1);
        if (rightKeyIndex < values.length) {
          rightFileIndex.put(values[rightKeyIndex], values);
        }
      }
    }

    Path outputFile = mergedFilesDir.resolve("merged_" + leftFile.getFileName() + "_" + rightFile.getFileName());

    // ===== 2. Prepara leitura do arquivo da esquerda (A) =====
    try (BufferedReader leftReader = Files.newBufferedReader(leftFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String leftHeaderLine = leftReader.readLine();
      if (leftHeaderLine == null) {
        throw new IllegalArgumentException("Arquivo da esquerda está vazio");
      }

      List<String> leftHeaders =
          Arrays.stream(leftHeaderLine.split(delimiter))
              .map(h -> h.replace("\"", "").trim())
              .toList();

      int leftKeyIndex = leftHeaders.indexOf(leftKeyColumn);
      if (leftKeyIndex == -1) {
        throw new IllegalArgumentException(
            "Chave '" + leftKeyColumn + "' não encontrada no arquivo da esquerda");
      }

      // ===== 3. Escreve cabeçalho final =====
      List<String> finalHeader = new ArrayList<>();
      finalHeader.addAll(leftHeaders);
      finalHeader.addAll(rightHeaders);
      finalHeader.add("OBSERVACAO");

      writer.write(String.join(delimiter, finalHeader));
      writer.newLine();

      // ===== 4. Processa linha a linha do arquivo da esquerda =====
      String leftLine;
      while ((leftLine = leftReader.readLine()) != null) {

        String[] leftValues = leftLine.split(delimiter, -1);
        String leftKeyValue = leftKeyIndex < leftValues.length ? leftValues[leftKeyIndex] : null;

        String[] rightValues = rightFileIndex.get(leftKeyValue);

        // adiciona colunas do arquivo da esquerda
        List<String> mergedLine = new ArrayList<>(Arrays.asList(leftValues));

        if (rightValues != null) {
          // chave encontrada → adiciona colunas do arquivo da direita
          mergedLine.addAll(Arrays.asList(rightValues));
          mergedLine.add("");
        } else {
          // chave não encontrada → completa com null
          for (int i = 0; i < rightHeaders.size(); i++) {
            mergedLine.add("null");
          }
          mergedLine.add("DADOS_NAO_ENCONTRADOS");
        }

        writer.write(String.join(delimiter, mergedLine));
        writer.newLine();
      }
    }
  }

  private double parseNumber(String[] values, int index) {
    if (index >= values.length) return 0.0;

    String raw = values[index].replace("\"", "").replace(",", ".").trim();

    if (raw.isEmpty()) return 0.0;

    try {
      return Double.parseDouble(raw);
    } catch (NumberFormatException e) {
      return 0.0;
    }
  }

  private double applyOperation(double v1, double v2, MathOperation operation) {

    return switch (operation) {
      case ADD -> v1 + v2;
      case SUBTRACT -> v1 - v2;
      case MULTIPLY -> v1 * v2;
      case DIVIDE -> v2 == 0 ? 0.0 : v1 / v2;
    };
  }

  private static Path createDirectory(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }
}
