package com.support;

import com.support.enums.ComparisonOperators;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

public class CsvValidator {

  private final Path file;
  private final String delimiter;
  private List<String[]> rows = new ArrayList<>();

  public CsvValidator(Path file, String delimiter) throws IOException {
    this.file = file;
    this.delimiter = delimiter;
    load();
  }

  /** Valida uma coluna por regex e cria coluna <COLUNA>_VALIDO */
  public CsvValidator validateByRegex(String column, String regex) {

    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    String newColumn = column.toUpperCase() + "_VALIDO";

    // adiciona header
    rows.set(0, append(rows.get(0), "\"" + newColumn + "\""));

    Pattern pattern = Pattern.compile(regex);

    for (int i = 1; i < rows.size(); i++) {
      String raw = clean(rows.get(i)[colIndex]);
      boolean valid = pattern.matcher(raw).matches();
      rows.set(i, append(rows.get(i), valid ? "true" : "false"));
    }

    return this;
  }

  /** Remove linhas onde o valor da coluna especificada é igual ao valor fornecido */
  public CsvValidator removeRowsByValue(String column, String valueToRemove) {

    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    List<String[]> filtered = new ArrayList<>();
    filtered.add(rows.get(0)); // mantém o header

    for (int i = 1; i < rows.size(); i++) {
      String cell = clean(rows.get(i)[colIndex]);
      if (!cell.equals(valueToRemove)) {
        filtered.add(rows.get(i));
      }
    }

    rows = filtered;
    return this;
  }

    /** Remove linhas com base em uma operação numérica */
  public CsvValidator filterRowsByNumericValue(
      String column, double value, ComparisonOperators op) {

    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    List<String[]> filtered = new ArrayList<>();
    filtered.add(rows.get(0)); // mantém o header

    for (int i = 1; i < rows.size(); i++) {
      String cellStr = clean(rows.get(i)[colIndex]);
      boolean keep = true; // por padrão mantém

      try {
        double cellValue = Double.parseDouble(cellStr);

        switch (op) {
          case EQ -> keep = cellValue != value; // remove se for igual
          case NE -> keep = cellValue == value; // remove se for diferente
          case LT -> keep = !(cellValue < value);
          case LE -> keep = !(cellValue <= value);
          case GT -> keep = !(cellValue > value);
          case GE -> keep = !(cellValue >= value);
        }

      } catch (NumberFormatException e) {
        // se não for número, mantém
      }

      if (keep) {
        filtered.add(rows.get(i));
      }
    }

    rows = filtered;
    return this;
  }

  /** Remove linhas onde <COLUNA>_VALIDO == false */
  public CsvValidator removeInvalidRows(String column) {

    String validationColumn = column.toUpperCase() + "_VALIDO";

    Map<String, Integer> index = headerIndex();
    Integer idx = index.get(validationColumn);

    if (idx == null) {
      throw new IllegalArgumentException("Coluna de validação não encontrada: " + validationColumn);
    }

    List<String[]> filtered = new ArrayList<>();
    filtered.add(rows.get(0)); // header

    for (int i = 1; i < rows.size(); i++) {
      if ("true".equalsIgnoreCase(clean(rows.get(i)[idx]))) {
        filtered.add(rows.get(i));
      }
    }

    rows = filtered;
    return this;
  }

  /** Valida a coluna de CNPJs e cria coluna <COLUNA>_VALIDO */
  public CsvValidator validateCnpj(String column) {

    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    String newColumn = column.toUpperCase() + "_VALIDO";

    // adiciona header
    rows.set(0, append(rows.get(0), "\"" + newColumn + "\""));

    for (int i = 1; i < rows.size(); i++) {
      String raw = clean(rows.get(i)[colIndex]);
      boolean valid = isCnpjValido(raw);
      rows.set(i, append(rows.get(i), valid ? "true" : "false"));
    }

    return this;
  }

  /** Função estática para validar CNPJs */
  public static boolean isCnpjValido(String cnpj) {
    if (cnpj == null) return false;

    // Remove caracteres não numéricos
    cnpj = cnpj.replaceAll("[^0-9]", "");

    // Verificações iniciais
    if (cnpj.length() != 14) return false;
    if (cnpj.matches("(\\d)\\1{13}")) return false;

    try {
      int sm, r, num, peso;
      char dig13, dig14;

      // 1º Dígito Verificador
      sm = 0;
      peso = 2;
      for (int i = 11; i >= 0; i--) {
        num = cnpj.charAt(i) - '0';
        sm += num * peso;
        peso++;
        if (peso == 10) peso = 2;
      }
      r = sm % 11;
      dig13 = (r < 2) ? '0' : (char) ((11 - r) + '0');

      // 2º Dígito Verificador
      sm = 0;
      peso = 2;
      for (int i = 12; i >= 0; i--) {
        num = cnpj.charAt(i) - '0';
        sm += num * peso;
        peso++;
        if (peso == 10) peso = 2;
      }
      r = sm % 11;
      dig14 = (r < 2) ? '0' : (char) ((11 - r) + '0');

      return dig13 == cnpj.charAt(12) && dig14 == cnpj.charAt(13);

    } catch (Exception e) {
      return false;
    }
  }

  public void save(Path output) throws IOException {

    Files.createDirectories(output.getParent());

    try (BufferedWriter writer =
        Files.newBufferedWriter(
            output,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      for (String[] row : rows) {
        writer.write(String.join(delimiter, row));
        writer.newLine();
      }
    }
  }

  private void load() throws IOException {
    rows.clear();
    rows.addAll(
        Files.readAllLines(file, StandardCharsets.UTF_8).stream()
            .map(line -> line.split(delimiter, -1))
            .toList());
  }

  private Map<String, Integer> headerIndex() {
    Map<String, Integer> map = new HashMap<>();
    String[] header = rows.get(0);

    for (int i = 0; i < header.length; i++) {
      map.put(clean(header[i]).toUpperCase(), i);
    }
    return map;
  }

  private String clean(String value) {
    return value == null ? "" : value.replace("\"", "").trim();
  }

  private String[] append(String[] original, String... values) {
    String[] result = Arrays.copyOf(original, original.length + values.length);
    System.arraycopy(values, 0, result, original.length, values.length);
    return result;
  }

  /**
   * Formata um campo para CSV: - Se contiver vírgula, aspas ou quebra de linha, envolve em aspas
   * duplas - Aspas internas são duplicadas
   */
  private String formatCsvField(String field) {
    if (field == null) return "";

    field = field.trim();

    // Se já começa e termina com aspas, assume que já está formatado
    if (field.startsWith("\"") && field.endsWith("\"")) {
      return field;
    }

    boolean containsComma = field.contains(",");

    // Tenta detectar se é número
    boolean isNumber;
    try {
      Double.parseDouble(field.replace(",", ".")); // aceita vírgula decimal
      isNumber = true;
    } catch (NumberFormatException e) {
      isNumber = false;
    }

    // Se não for número ou contiver vírgula, adiciona aspas
    if (!isNumber || containsComma) {
      String escaped = field.replace("\"", "\"\""); // escapa aspas internas
      return "\"" + escaped + "\"";
    }

    return field; // número sem vírgula
  }

  public void saveFormatted(String fileName) throws IOException {
    // Cria diretório "output" na raiz do projeto
    Path outputDir = Path.of("output");
    Files.createDirectories(outputDir);

    // Cria o caminho completo para o arquivo
    Path outputFile = outputDir.resolve(fileName);

    try (BufferedWriter writer =
        Files.newBufferedWriter(
            outputFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

      for (String[] row : rows) {
        // Aplica formatação em cada campo
        String line =
            Arrays.stream(row)
                .map(this::formatCsvField)
                .reduce((a, b) -> a + delimiter + b)
                .orElse("");
        writer.write(line);
        writer.newLine();
      }
    }

    System.out.println("CSV salvo em: " + outputFile.toAbsolutePath());
  }

  /** Valida se um campo obrigatório está vazio. Marca observação e cria coluna <COLUNA>_VALIDO */
  public CsvValidator validateRequiredField(String column) {

    Map<String, Integer> index = headerIndex();
    Integer colIndex = index.get(column.toUpperCase());

    if (colIndex == null) {
      throw new IllegalArgumentException("Coluna não encontrada: " + column);
    }

    String validColumn = column.toUpperCase() + "_VALIDO";
    String obsColumn = "OBSERVACAO";

    // === HEADER ===
    List<String> header = new ArrayList<>(List.of(rows.get(0)));

    if (!header.contains(validColumn)) {
      header.add("\"" + validColumn + "\"");
    }

    if (!header.contains(obsColumn)) {
      header.add("\"" + obsColumn + "\"");
    }

    rows.set(0, header.toArray(new String[0]));

    // Atualiza índices após alterar header
    index = headerIndex();
    int validIdx = index.get(validColumn);
    int obsIdx = index.get(obsColumn);

    // === LINHAS ===
    for (int i = 1; i < rows.size(); i++) {

      String[] row = rows.get(i);
      String value = clean(row[colIndex]);

      boolean isValid = !value.isEmpty();

      row = ensureSize(row, rows.get(0).length);

      row[validIdx] = isValid ? "true" : "false";

      if (!isValid) {
        String obs = clean(row[obsIdx]);
        String msg = "Campo " + column + " não preenchido";
        row[obsIdx] = obs.isEmpty() ? msg : obs + " | " + msg;
      }

      rows.set(i, row);
    }

    return this;
  }

  private String[] ensureSize(String[] row, int size) {
    if (row.length >= size) return row;
    return Arrays.copyOf(row, size);
  }
}
