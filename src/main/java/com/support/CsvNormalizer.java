package com.support;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

public class CsvNormalizer {

  private Path file; // arquivo CSV original ou atualizado
  private final String delimiter; // delimitador do CSV
  private final Path normalizedOutputDir; // pasta onde arquivos normalizados serão salvos
  private List<String[]> rows = new ArrayList<>(); // linhas do CSV

  public CsvNormalizer(Path file, String delimiter) throws IOException {
    this.file = file;
    this.delimiter = delimiter;

    // Cria diretório normalized_files/ se não existir
    this.normalizedOutputDir = Paths.get(System.getProperty("user.dir"), "normalized_files");
    Files.createDirectories(this.normalizedOutputDir);

    load(); // carrega CSV na memória
  }

  /** Normaliza o header: remove aspas, trim e converte para uppercase */
  public CsvNormalizer normalizeHeaders() {
    String[] header = rows.get(0);

    for (int i = 0; i < header.length; i++) {
      header[i] = clean(header[i]).toUpperCase();
    }
    return this;
  }

  /** Normaliza números: substitui "," por ".", remove aspas */
  public CsvNormalizer normalizeNumbers() {
    for (int i = 1; i < rows.size(); i++) {
      String[] row = rows.get(i);

      for (int j = 0; j < row.length; j++) {
        String raw = clean(row[j]);
        if (raw.matches("-?\\d+[.,]?\\d*")) {
          row[j] = raw.replace(",", ".");
        }
      }
    }
    return this;
  }

  /** Normaliza colunas textuais específicas: remove espaços e converte para lowercase */
  public CsvNormalizer normalizeTextColumns(List<String> columns) {
    Map<String, Integer> index = headerIndex();

    for (String col : columns) {
      Integer idx = index.get(col.toUpperCase());
      if (idx == null) continue;

      for (int i = 1; i < rows.size(); i++) {
        rows.get(i)[idx] = clean(rows.get(i)[idx]).toLowerCase();
      }
    }
    return this;
  }

  /** Remove linhas completamente vazias */
  public CsvNormalizer removeBlankLines() {
    rows =
        rows.stream()
            .filter(r -> Arrays.stream(r).anyMatch(v -> v != null && !clean(v).isEmpty()))
            .collect(Collectors.toList());
    return this;
  }

  /** Salva o CSV normalizado na pasta normalized_files/ */
  public CsvNormalizer save() throws IOException {
    Path outputFile = normalizedOutputDir.resolve(file.getFileName());

    try (BufferedWriter writer =
        Files.newBufferedWriter(
            outputFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      for (int i = 0; i < rows.size(); i++) {
        String[] row = rows.get(i);
        List<String> formatted = new ArrayList<>();

        for (String value : row) {
          String cleanValue = clean(value);

          // Header: SEMPRE com aspas
          if (i == 0) {
            formatted.add("\"" + cleanValue + "\"");
          }
          // Dados numéricos: SEM aspas
          else if (isNumeric(cleanValue)) {
            formatted.add(cleanValue);
          }
          // Dados textuais: COM aspas
          else {
            formatted.add("\"" + cleanValue + "\"");
          }
        }

        writer.write(String.join(delimiter, formatted));
        writer.newLine();
      }
    }

    // Atualiza referência do arquivo para o normalizado
    this.file = outputFile;
    return this;
  }

  /** Filtra linhas com base em valor exato de uma coluna */
  public CsvNormalizer filterByColumnValue(String column, String expectedValue) {
    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    String expected = clean(expectedValue).replaceAll("\\s+", "").toLowerCase();
    List<String[]> filtered = new ArrayList<>();
    filtered.add(rows.get(0)); // mantém o header

    for (int i = 1; i < rows.size(); i++) {
      String raw = rows.get(i)[colIndex];
      String normalized = clean(raw).replaceAll("\\s+", "").toLowerCase();

      if (normalized.equals(expected)) {
        filtered.add(rows.get(i));
      }
    }

    rows = filtered;
    return this;
  }

  /** Carrega CSV na memória */
  private void load() throws IOException {
    rows.clear();
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
      map.put(clean(header[i]).toUpperCase(), i);
    }
    return map;
  }

  /** Remove aspas e espaços do valor */
  private String clean(String value) {
    return value == null ? "" : value.replace("\"", "").trim();
  }

  /** Verifica se o valor é numérico */
  private boolean isNumeric(String value) {
    if (value == null) return false;

    String raw = value.replace("\"", "").trim();
    if (raw.isEmpty()) return false;

    return raw.matches("-?\\d+(\\.\\d+)?");
  }
}
