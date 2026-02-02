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

  // Diretório onde serão salvos os arquivos com validação por regex
  private final Path validatedFilesDir;

  // Diretório onde serão salvos os arquivos mesclados
  private final Path mergedFilesDir;

  // Diretório onde serão salvos os arquivos com colunas calculadas
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

  // Lê o cabeçalho do CSV e retorna um mapa com o nome da coluna e seu índice
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

  // Valida uma coluna usando regex e adiciona uma coluna indicando se o valor é válido
  public void validateColumnByRegex(
      Path inputFile, String columnName, String regex, String delimiter) throws IOException {

    // Obtém o índice das colunas do CSV
    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    // Verifica se a coluna existe
    Integer columnIndex = headerIndex.get(columnName);
    if (columnIndex == null) {
      throw new IllegalArgumentException("Coluna '" + columnName + "' não encontrada no CSV");
    }

    // Compila o regex informado
    Pattern pattern = Pattern.compile(regex);

    // Define o arquivo de saída com prefixo validated_
    Path outputFile = validatedFilesDir.resolve(inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      // Lê o cabeçalho original
      String headerLine = reader.readLine();

      // Escreve o cabeçalho original adicionando a nova coluna de validação
      writer.write((headerLine + delimiter + columnName + "_VALIDO").toUpperCase());
      writer.newLine();

      String line;

      // Processa cada linha do arquivo
      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        String valueToValidate = columnIndex < values.length ? values[columnIndex] : "";

        // Normaliza o valor antes da validação
        valueToValidate = valueToValidate.replace("\"", "").trim();

        boolean matches = pattern.matcher(valueToValidate).matches();

        // Escreve a linha original adicionando 1 ou 0 conforme o resultado do regex
        writer.write(line + delimiter + (matches ? "1" : "0"));
        writer.newLine();
      }
    }
  }

  // Cria uma nova coluna calculada a partir de duas colunas existentes
  public void calculateNewColumn(
      Path inputFile,
      String column1,
      String column2,
      String newColumnName,
      MathOperation operation,
      String delimiter)
      throws IOException {

    // Obtém o índice das colunas do CSV
    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    Integer index1 = headerIndex.get(column1);
    Integer index2 = headerIndex.get(column2);

    if (index1 == null || index2 == null) {
      throw new IllegalArgumentException("Colunas informadas não existem no CSV");
    }

    // Define o arquivo de saída com prefixo calculated_
    Path outputFile = calculetedFilesDir.resolve(inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
        BufferedWriter writer =
            Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

      // Lê o cabeçalho original
      String headerLine = reader.readLine();

      // Escreve o cabeçalho adicionando a nova coluna calculada
      writer.write((headerLine + delimiter + newColumnName).toUpperCase());
      writer.newLine();

      String line;

      // Processa cada linha do arquivo
      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        double value1 = parseNumber(values, index1);
        double value2 = parseNumber(values, index2);

        double result = applyOperation(value1, value2, operation);

        String resultFormatado = String.format("%.2f", result);

        // Escreve a linha original adicionando o resultado da operação
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

  // Mescla dois arquivos CSV utilizando uma chave comum
  public void mergeCsvByKey(
      Path leftFile, Path rightFile, String leftKeyColumn, String rightKeyColumn, String delimiter)
      throws IOException {

    // Indexa o arquivo da direita usando a chave informada
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

    String leftName = leftFile.getFileName().toString().replaceFirst("\\.csv$", "");
    String rightName = rightFile.getFileName().toString().replaceFirst("\\.csv$", "");

    Path outputFile = mergedFilesDir.resolve(leftName + "_" + rightName + ".csv");

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

      // Escreve o cabeçalho final com colunas dos dois arquivos
      List<String> finalHeader = new ArrayList<>();
      finalHeader.addAll(leftHeaders);
      finalHeader.addAll(rightHeaders);
      finalHeader.add("OBSERVACAO");

      writer.write(String.join(delimiter, finalHeader));
      writer.newLine();

      // Processa cada linha do arquivo da esquerda
      String leftLine;
      while ((leftLine = leftReader.readLine()) != null) {

        String[] leftValues = leftLine.split(delimiter, -1);
        String leftKeyValue = leftKeyIndex < leftValues.length ? leftValues[leftKeyIndex] : null;

        String[] rightValues = rightFileIndex.get(leftKeyValue);

        List<String> mergedLine = new ArrayList<>(Arrays.asList(leftValues));

        if (rightValues != null) {
          mergedLine.addAll(Arrays.asList(rightValues));
          mergedLine.add("");
        } else {
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

  public void extractColumns(Path inputFile, List<String> columnsToKeep, String delimiter)
          throws IOException {

    // Lê o índice do cabeçalho
    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    // Valida se todas as colunas existem
    for (String column : columnsToKeep) {
      if (!headerIndex.containsKey(column)) {
        throw new IllegalArgumentException("Coluna '" + column + "' não encontrada no CSV");
      }
    }

    // Diretório de saída
    Path outputDir = Paths.get(System.getProperty("user.dir"), "projected_files");
    Files.createDirectories(outputDir);

    // Arquivo de saída (mesmo nome do original)
    Path outputFile = outputDir.resolve(inputFile.getFileName());

    // Índices das colunas que serão extraídas
    List<Integer> columnIndexes =
            columnsToKeep.stream().map(headerIndex::get).toList();

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
         BufferedWriter writer =
                 Files.newBufferedWriter(
                         outputFile,
                         StandardCharsets.UTF_8,
                         StandardOpenOption.CREATE,
                         StandardOpenOption.TRUNCATE_EXISTING)) {

      // Escreve o novo cabeçalho
      writer.write(String.join(delimiter, columnsToKeep));
      writer.newLine();

      // DESCARTA o cabeçalho original do CSV
      reader.readLine();

      String line;

      // Processa apenas as linhas de dados
      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        List<String> projectedValues = new ArrayList<>();

        for (int index : columnIndexes) {
          projectedValues.add(index < values.length ? values[index] : "");
        }

        writer.write(String.join(delimiter, projectedValues));
        writer.newLine();
      }
    }
  }


  // Converte um valor do CSV para double de forma segura
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

  // Aplica a operação matemática informada
  private double applyOperation(double v1, double v2, MathOperation operation) {
    return switch (operation) {
      case ADD -> v1 + v2;
      case SUBTRACT -> v1 - v2;
      case MULTIPLY -> v1 * v2;
      case DIVIDE -> v2 == 0 ? 0.0 : v1 / v2;
    };
  }

  // Cria um diretório na raiz do projeto caso não exista
  private static Path createDirectory(String folderName) throws IOException {
    Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
    Files.createDirectories(baseDir);
    return baseDir;
  }

  public void addYearAndQuarterColumns(
          Path inputFile,
          String dateColumnName,
          String delimiter
  ) throws IOException {

    Map<String, Integer> headerIndex = readCsvHeaderIndex(inputFile, delimiter);

    Integer dateIndex = headerIndex.get(dateColumnName);
    if (dateIndex == null) {
      throw new IllegalArgumentException(
              "Coluna de data '" + dateColumnName + "' não encontrada no CSV");
    }

    Path outputFile = calculetedFilesDir.resolve("new_" + inputFile.getFileName());

    try (BufferedReader reader = Files.newBufferedReader(inputFile, StandardCharsets.UTF_8);
         BufferedWriter writer = Files.newBufferedWriter(
                 outputFile,
                 StandardCharsets.UTF_8,
                 StandardOpenOption.CREATE,
                 StandardOpenOption.TRUNCATE_EXISTING)) {

      // Cabeçalho original
      String headerLine = reader.readLine();

      // Novo cabeçalho
      writer.write((headerLine + delimiter + "ANO" + delimiter + "TRIMESTRE").toUpperCase());
      writer.newLine();

      String line;

      while ((line = reader.readLine()) != null) {

        if (line.trim().isEmpty()) continue;

        String[] values = line.split(delimiter, -1);

        String rawDate = dateIndex < values.length
                ? values[dateIndex].replace("\"", "").trim()
                : "";

        int year = 0;
        int quarter = 0;

        // Validação básica do formato yyyy-mm-dd
        if (rawDate.matches("\\d{4}-\\d{2}-\\d{2}")) {

          String[] dateParts = rawDate.split("-");

          year = Integer.parseInt(dateParts[0]);
          int month = Integer.parseInt(dateParts[1]);

          quarter = ((month - 1) / 3) + 1;
        }

        writer.write(
                line
                        + delimiter
                        + year
                        + delimiter
                        + (quarter == 0 ? "" : "Q" + quarter)
        );
        writer.newLine();
      }
    }
  }
}
