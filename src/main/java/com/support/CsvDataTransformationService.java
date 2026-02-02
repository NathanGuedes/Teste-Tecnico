package com.support;

import com.support.enums.MathOperation;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CsvDataTransformationService {

  private final Path validatedFilesDir;
  private final Path mergedFilesDir;
  private final Path calculatedFilesDir;

  public CsvDataTransformationService() throws IOException {
    this.validatedFilesDir = createDirectory("validated_files");
    this.mergedFilesDir = createDirectory("merged_files");
    this.calculatedFilesDir = createDirectory("calculated_files");
  }

  public Path getValidatedFilesDir() {
    return validatedFilesDir;
  }

  public Path getMergedFilesDir() {
    return mergedFilesDir;
  }

  public Path getCalculatedFilesDir() {
    return calculatedFilesDir;
  }

  private static Map<String, Integer> readCsvHeaderIndex(Path csvFile, String delimiter)
      throws IOException {

    try (BufferedReader reader = Files.newBufferedReader(csvFile, StandardCharsets.UTF_8)) {

      String header = reader.readLine();
      if (header == null) {
        throw new IllegalArgumentException("Arquivo CSV vazio");
      }

      String[] columns = header.split(delimiter);
      Map<String, Integer> index = new LinkedHashMap<>();

      for (int i = 0; i < columns.length; i++) {
        index.put(columns[i].replace("\"", "").trim(), i);
      }

      return index;
    }
  }

  public void validateColumnByRegex(
      Path inputFile, String columnName, String regex, String delimiter) throws IOException {

    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);
    Integer columnIndex = headerIndex.get(columnName);

    if (columnIndex == null) {
      throw new IllegalArgumentException("Coluna '" + columnName + "' não encontrada");
    }

    Pattern pattern = Pattern.compile(regex);
    Path outputFile = validatedFilesDir.resolve(inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String header = reader.readLine();
      writer.write((header + delimiter + columnName + "_VALIDO").toUpperCase());
      writer.newLine();

      String line;
      while ((line = reader.readLine()) != null) {

        if (line.isBlank()) continue;

        String[] values = line.split(delimiter, -1);
        String value =
            columnIndex < values.length ? values[columnIndex].replace("\"", "").trim() : "";

        writer.write(line + delimiter + (pattern.matcher(value).matches() ? "1" : "0"));
        writer.newLine();
      }
    }
  }

  public void calculateNewColumn(
      Path inputFile,
      String column1,
      String column2,
      String newColumn,
      MathOperation operation,
      String delimiter)
      throws IOException {

    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    Integer idx1 = headerIndex.get(column1);
    Integer idx2 = headerIndex.get(column2);

    if (idx1 == null || idx2 == null) {
      throw new IllegalArgumentException("Colunas informadas não existem");
    }

    Path outputFile = calculatedFilesDir.resolve(inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String header = reader.readLine();
      writer.write((header + delimiter + newColumn).toUpperCase());
      writer.newLine();

      String line;
      while ((line = reader.readLine()) != null) {

        if (line.isBlank()) continue;

        String[] values = line.split(delimiter, -1);

        double v1 = parseNumber(values, idx1);
        double v2 = parseNumber(values, idx2);

        double result = applyOperation(v1, v2, operation);
        String formatted = String.format("%.2f", result);

        if (delimiter.equals(";")) {
          formatted = formatted.replace(".", ",");
        }

        writer.write(line + delimiter + "\"" + formatted + "\"");
        writer.newLine();
      }
    }
  }

  public void mergeCsvByKey(
      Path leftFile, Path rightFile, String leftKey, String rightKey, String delimiter)
      throws IOException {

    Map<String, String[]> rightIndex = new HashMap<>();
    List<String> rightHeaders;
    int rightKeyIndex;

    try (BufferedReader reader = Files.newBufferedReader(rightFile, StandardCharsets.UTF_8)) {

      String header = reader.readLine();
      rightHeaders =
          Arrays.stream(header.split(delimiter)).map(h -> h.replace("\"", "").trim()).toList();

      rightKeyIndex = rightHeaders.indexOf(rightKey);
      if (rightKeyIndex == -1) {
        throw new IllegalArgumentException("Chave não encontrada no CSV da direita");
      }

      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(delimiter, -1);
        if (rightKeyIndex < values.length) {
          rightIndex.put(values[rightKeyIndex], values);
        }
      }
    }

    Path output =
        mergedFilesDir.resolve(
            leftFile.getFileName().toString().replace(".csv", "")
                + "_"
                + rightFile.getFileName().toString());

    try (BufferedReader leftReader = Files.newBufferedReader(leftFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                output,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String leftHeader = leftReader.readLine();
      List<String> leftHeaders =
          Arrays.stream(leftHeader.split(delimiter)).map(h -> h.replace("\"", "").trim()).toList();

      int leftKeyIndex = leftHeaders.indexOf(leftKey);
      if (leftKeyIndex == -1) {
        throw new IllegalArgumentException("Chave não encontrada no CSV da esquerda");
      }

      List<String> finalHeader = new ArrayList<>(leftHeaders);
      finalHeader.addAll(rightHeaders);
      finalHeader.add("OBSERVACAO");

      writer.write(String.join(delimiter, finalHeader));
      writer.newLine();

      String line;
      while ((line = leftReader.readLine()) != null) {

        String[] leftValues = line.split(delimiter, -1);
        String key = leftKeyIndex < leftValues.length ? leftValues[leftKeyIndex] : null;

        String[] rightValues = rightIndex.get(key);
        List<String> merged = new ArrayList<>(Arrays.asList(leftValues));

        if (rightValues != null) {
          merged.addAll(Arrays.asList(rightValues));
          merged.add("");
        } else {
          merged.addAll(Collections.nCopies(rightHeaders.size(), ""));
          merged.add("DADOS_NAO_ENCONTRADOS");
        }

        writer.write(String.join(delimiter, merged));
        writer.newLine();
      }
    }
  }

  public void extractColumns(Path inputFile, List<String> columns, String delimiter)
      throws IOException {

    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    for (String col : columns) {
      if (!headerIndex.containsKey(col)) {
        throw new IllegalArgumentException("Coluna '" + col + "' não encontrada");
      }
    }

    Path outputFile =
        Paths.get(System.getProperty("user.dir"), "output", "consolidado_despesas.csv");

    Files.createDirectories(outputFile.getParent());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      writer.write(String.join(delimiter, columns));
      writer.newLine();

      reader.readLine(); // descarta header

      String line;
      while ((line = reader.readLine()) != null) {

        String[] values = line.split(delimiter, -1);
        List<String> out = new ArrayList<>();

        for (String col : columns) {
          int idx = headerIndex.get(col);
          out.add(idx < values.length ? values[idx] : "");
        }

        writer.write(String.join(delimiter, out));
        writer.newLine();
      }
    }
  }

  public void addYearAndQuarterColumns(Path inputFile, String dateColumn, String delimiter)
      throws IOException {

    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);
    Integer dateIndex = headerIndex.get(dateColumn);

    if (dateIndex == null) {
      throw new IllegalArgumentException("Coluna de data não encontrada");
    }

    Path output = calculatedFilesDir.resolve("new_" + inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                output,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      String header = reader.readLine();
      writer.write((header + delimiter + "ANO" + delimiter + "TRIMESTRE").toUpperCase());
      writer.newLine();

      String line;
      while ((line = reader.readLine()) != null) {

        String[] values = line.split(delimiter, -1);
        String raw = dateIndex < values.length ? values[dateIndex].replace("\"", "").trim() : "";

        int year = 0;
        String quarter = "";

        if (raw.matches("\\d{4}-\\d{2}-\\d{2}")) {
          int month = Integer.parseInt(raw.substring(5, 7));
          year = Integer.parseInt(raw.substring(0, 4));
          quarter = "Q" + (((month - 1) / 3) + 1);
        }

        writer.write(line + delimiter + year + delimiter + quarter);
        writer.newLine();
      }
    }
  }

  public Path zipPathKeepingName(Path source) throws IOException {

    if (!Files.exists(source)) {
      throw new IllegalArgumentException("Caminho não existe: " + source);
    }

    Path zipPath = source.getParent().resolve(source.getFileName().toString() + ".zip");

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {

      if (Files.isDirectory(source)) {

        try (Stream<Path> paths = Files.walk(source)) {
          paths
              .filter(Files::isRegularFile)
              .forEach(
                  path -> {
                    try {
                      ZipEntry entry = new ZipEntry(source.relativize(path).toString());
                      zos.putNextEntry(entry);
                      Files.copy(path, zos);
                      zos.closeEntry();
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });
        }

      } else {
        zos.putNextEntry(new ZipEntry(source.getFileName().toString()));
        Files.copy(source, zos);
        zos.closeEntry();
      }
    }

    return zipPath;
  }

  private static Path createDirectory(String name) throws IOException {
    Path dir = Paths.get(System.getProperty("user.dir"), name);
    Files.createDirectories(dir);
    return dir;
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

  private double applyOperation(double a, double b, MathOperation op) {
    return switch (op) {
      case ADD -> a + b;
      case SUBTRACT -> a - b;
      case MULTIPLY -> a * b;
      case DIVIDE -> b == 0 ? 0.0 : a / b;
    };
  }
}
