package com.support;

import com.support.enums.MathOperation;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class CsvTransformer {

  private Path file; // arquivo CSV original ou atualizado
  private final String delimiter; // delimitador do CSV
  private List<String[]> rows = new ArrayList<>(); // linhas do CSV

  /** Construtor: carrega o CSV na memória */
  public CsvTransformer(Path file, String delimiter) throws IOException {
    this.file = file;
    this.delimiter = delimiter;
    load();
  }

  /** Cria uma nova coluna baseada em duas existentes com operação matemática */
  public CsvTransformer calculateNewColumn(
      String colA, String colB, String newColumn, MathOperation operation) {

    Map<String, Integer> index = headerIndex();
    Integer idxA = index.get(colA.toUpperCase());
    Integer idxB = index.get(colB.toUpperCase());

    if (idxA == null || idxB == null) {
      throw new IllegalArgumentException("Colunas informadas não existem");
    }

    // Adiciona nova coluna no header
    rows.set(0, append(rows.get(0), newColumn));

    // Calcula valor para cada linha
    for (int i = 1; i < rows.size(); i++) {
      double a = parseNumber(rows.get(i), idxA);
      double b = parseNumber(rows.get(i), idxB);
      double result = applyOperation(a, b, operation);
      rows.set(i, append(rows.get(i), String.format(Locale.US, "%.2f", result)));
    }

    return this;
  }

  /** Adiciona colunas ANO e TRIMESTRE a partir de uma coluna de data (yyyy-MM-dd) */
  public CsvTransformer addYearAndQuarterColumns(String dateColumn) {

    Map<String, Integer> index = headerIndex();
    Integer dateIdx = index.get(dateColumn.toUpperCase());

    if (dateIdx == null) {
      throw new IllegalArgumentException("Coluna de data não encontrada");
    }

    // Adiciona colunas no header
    rows.set(0, append(rows.get(0), "\"ANO\"", "\"TRIMESTRE\""));

    for (int i = 1; i < rows.size(); i++) {
      String raw = rows.get(i)[dateIdx].replace("\"", "").trim();
      int year = 0;
      String quarter = "";

      if (raw.matches("\\d{4}-\\d{2}-\\d{2}")) {
        int month = Integer.parseInt(raw.substring(5, 7));
        year = Integer.parseInt(raw.substring(0, 4));
        quarter = "Q" + (((month - 1) / 3) + 1);
      }

      rows.set(i, append(rows.get(i), String.valueOf(year), quarter));
    }

    return this;
  }

  /** Mantém apenas as colunas informadas */
  public CsvTransformer extractColumns(List<String> columns) {

    Map<String, Integer> index = headerIndex();
    List<Integer> indexes = new ArrayList<>();

    for (String col : columns) {
      Integer idx = index.get(col.toUpperCase());
      if (idx == null) {
        throw new IllegalArgumentException("Coluna não encontrada: " + col);
      }
      indexes.add(idx);
    }

    // Filtra linhas mantendo apenas as colunas selecionadas
    List<String[]> newRows = new ArrayList<>();
    for (String[] row : rows) {
      String[] filtered = new String[indexes.size()];
      for (int i = 0; i < indexes.size(); i++) {
        filtered[i] = row[indexes.get(i)];
      }
      newRows.add(filtered);
    }

    rows = newRows;
    return this;
  }

  /** Concatena múltiplos CSVs em um único */
  public static CsvTransformer concatCsvFiles(List<Path> files, String delimiter)
      throws IOException {

    if (files == null || files.isEmpty()) {
      throw new IllegalArgumentException("Lista de arquivos vazia");
    }

    // Usa o primeiro arquivo como base (header + dados)
    CsvTransformer transformer = new CsvTransformer(files.get(0), delimiter);

    // Adiciona linhas dos demais arquivos (descartando header)
    for (int i = 1; i < files.size(); i++) {
      try (BufferedReader reader = Files.newBufferedReader(files.get(i), StandardCharsets.UTF_8)) {
        reader.readLine(); // descarta header
        String line;
        while ((line = reader.readLine()) != null) {
          transformer.rows.add(line.split(delimiter, -1));
        }
      }
    }

    return transformer;
  }

  /** Mescla CSV atual com outro CSV usando chaves específicas */
  public CsvTransformer mergeByKey(Path rightFile, String leftKey, String rightKey)
      throws IOException {

    Map<String, String[]> rightIndex = new HashMap<>();
    List<String> rightHeaders;
    int rightKeyIndex;

    try (BufferedReader reader = Files.newBufferedReader(rightFile, StandardCharsets.UTF_8)) {
      String header = reader.readLine();
      rightHeaders = Arrays.stream(header.split(delimiter, -1)).map(this::clean).toList();
      rightKeyIndex = rightHeaders.indexOf(rightKey.toUpperCase());
      if (rightKeyIndex == -1) {
        throw new IllegalArgumentException("Chave não encontrada no CSV da direita: " + rightKey);
      }

      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(delimiter, -1);
        String key = values[rightKeyIndex].replace("\"", "").trim();
        rightIndex.put(key, values);
      }
    }

    List<String> leftHeaders = Arrays.stream(rows.get(0)).map(this::clean).toList();
    int leftKeyIndex = leftHeaders.indexOf(leftKey.toUpperCase());
    if (leftKeyIndex == -1) {
      throw new IllegalArgumentException("Chave não encontrada no CSV da esquerda: " + leftKey);
    }

    List<String[]> mergedRows = new ArrayList<>();
    List<String> finalHeader = new ArrayList<>(leftHeaders);
    finalHeader.addAll(rightHeaders);
    finalHeader.add("OBSERVACAO"); // coluna extra para observações
    mergedRows.add(finalHeader.toArray(String[]::new));

    for (int i = 1; i < rows.size(); i++) {
      String[] leftValues = rows.get(i);
      String key = leftValues[leftKeyIndex].replace("\"", "").trim();

      String[] rightValues = rightIndex.get(key);
      List<String> merged = new ArrayList<>(Arrays.asList(leftValues));

      if (rightValues != null) {
        merged.addAll(Arrays.asList(rightValues));
        merged.add(""); // observação vazia
      } else {
        merged.addAll(Collections.nCopies(rightHeaders.size(), ""));
        merged.add("DADOS_NAO_ENCONTRADOS");
      }

      mergedRows.add(merged.toArray(String[]::new));
    }

    this.rows = mergedRows;
    return this;
  }

  /** Salva CSV transformado no caminho informado */
  public void save(Path outputFile) throws IOException {
    Files.createDirectories(outputFile.getParent());
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            outputFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      for (String[] row : rows) {
        writer.write(String.join(delimiter, row));
        writer.newLine();
      }
    }
    this.file = outputFile;
  }

  /** Carrega CSV na memória */
  private void load() throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        rows.add(line.split(delimiter, -1));
      }
    }
  }

  /** Cria mapa de índice de colunas (nome da coluna -> posição) */
  private Map<String, Integer> headerIndex() {
    Map<String, Integer> map = new HashMap<>();
    String[] header = rows.get(0);
    for (int i = 0; i < header.length; i++) {
      map.put(header[i].replace("\"", "").trim().toUpperCase(), i);
    }
    return map;
  }

  /** Adiciona colunas no final da linha */
  private String[] append(String[] row, String... values) {
    String[] out = Arrays.copyOf(row, row.length + values.length);
    System.arraycopy(values, 0, out, row.length, values.length);
    return out;
  }

  /** Converte valor de célula para número */
  private double parseNumber(String[] row, int idx) {
    try {
      return Double.parseDouble(row[idx].replace("\"", "").replace(",", ".").trim());
    } catch (Exception e) {
      return 0.0;
    }
  }

  /** Aplica operação matemática */
  private double applyOperation(double a, double b, MathOperation op) {
    return switch (op) {
      case ADD -> a + b;
      case SUBTRACT -> a - b;
      case MULTIPLY -> a * b;
      case DIVIDE -> b == 0 ? 0.0 : a / b;
    };
  }

  /** Limpa valor removendo aspas, trim e converte para uppercase */
  private String clean(String value) {
    return value == null ? "" : value.replace("\"", "").trim().toUpperCase();
  }
}
