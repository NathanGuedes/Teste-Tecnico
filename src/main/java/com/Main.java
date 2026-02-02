package com;

import com.support.*;
import com.support.enums.MathOperation;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Main {

  public static void main(String[] args) throws IOException {

    ZipArchiveService zipService = buildZipArchiveService();
    zipService.downloadAndExtractArchives();

    Path extractDir = Path.of(System.getProperty("user.dir"), "extract");

    List<Path> extractedFiles;
    try (Stream<Path> paths = Files.list(extractDir)) {
      extractedFiles = paths.filter(Files::isRegularFile).toList();
    }

    CsvFileProcessor csvProcessor = new CsvFileProcessor();

    csvProcessor.filterCsvFilesByColumnValue(
        extractedFiles, "DESCRICAO", "Despesas com Eventos/Sinistros", ";");

    Path filteredDir = csvProcessor.getFilteredOutputDir();

    List<Path> filteredFiles;
    try (Stream<Path> paths = Files.list(filteredDir)) {
      filteredFiles = paths.filter(Files::isRegularFile).toList();
    }

    csvProcessor.mergeCsvFiles(filteredFiles);

    Path preProcessedDir = csvProcessor.getPreProcessedOutputDir();
    Path consolidatedFile = preProcessedDir.resolve("quarters_by_description.csv");

    csvProcessor.removeDuplicateLines(consolidatedFile, true);

    Path extraFilesDir = Path.of(System.getProperty("user.dir"), "extra_files");
    Path operatorsFile = extraFilesDir.resolve("dados operadoras.csv");

    csvProcessor.removeDuplicateLines(operatorsFile, true);

    CsvDataTransformationService csvService = new CsvDataTransformationService();

    Path validatedOperators = preProcessedDir.resolve("unique_dados operadoras.csv");

    csvService.validateColumnByRegex(
        validatedOperators, "CNPJ", "^\\d{2}\\.?\\d{3}\\.?\\d{3}\\/?\\d{4}-?\\d{2}$", ";");

    Path expensesCsv = preProcessedDir.resolve("unique_quarters_by_description.csv");

    csvService.calculateNewColumn(
        expensesCsv,
        "VL_SALDO_FINAL",
        "VL_SALDO_INICIAL",
        "VALOR_DESPESAS",
        MathOperation.SUBTRACT,
        ";");

    Path calculatedDir = csvService.getCalculatedFilesDir();
    Path validatedDir = csvService.getValidatedFilesDir();

    csvService.addYearAndQuarterColumns(
        calculatedDir.resolve("unique_quarters_by_description.csv"), "DATA", ";");

    Path leftFile = calculatedDir.resolve("new_unique_quarters_by_description.csv");

    Path rightFile = validatedDir.resolve("unique_dados operadoras.csv");

    csvService.mergeCsvByKey(leftFile, rightFile, "REG_ANS", "REGISTRO_OPERADORA", ";");

    Path mergedDir = csvService.getMergedFilesDir();

    Path mergedFile =
        mergedDir.resolve("new_unique_quarters_by_description_unique_dados operadoras.csv");

    csvService.extractColumns(
        mergedFile,
        List.of(
            "DATA",
            "CNPJ",
            "RAZAO_SOCIAL",
            "DESCRICAO",
            "TRIMESTRE",
            "ANO",
            "VALOR_DESPESAS",
            "REG_ANS",
            "MODALIDADE",
            "UF",
            "CNPJ_VALIDO",
            "OBSERVACAO"),
        ";");

    zipFiles(Path.of("output", "consolidado_despesas.csv"));

    deleteDirectoryFromProjectRoot("validated_files");
    deleteDirectoryFromProjectRoot("merged_files");
    deleteDirectoryFromProjectRoot("calculated_files");
    deleteDirectoryFromProjectRoot("filtered_files");
    deleteDirectoryFromProjectRoot("pre_processed_files");
  }

  private static ZipArchiveService buildZipArchiveService() throws IOException {

    String baseUrl = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

    QuarterlyReportUrlScraper scraper = new QuarterlyReportUrlScraper();
    List<String> reports = scraper.fetchLatestQuarterReportUrls(baseUrl, 3);

    return new ZipArchiveService(baseUrl, reports);
  }

  public static void deleteDirectoryFromProjectRoot(String folderName) throws IOException {

    Path dir = Path.of(System.getProperty("user.dir"), folderName);

    if (!Files.exists(dir)) return;

    Files.walk(dir)
        .sorted(Comparator.reverseOrder())
        .forEach(
            path -> {
              try {
                Files.delete(path);
              } catch (IOException e) {
                throw new RuntimeException("Erro ao apagar: " + path, e);
              }
            });
  }

  public static Path zipFiles(Path sourcePath) throws IOException {

    if (!Files.exists(sourcePath)) {
      throw new IllegalArgumentException("Caminho não existe: " + sourcePath);
    }

    String baseName = sourcePath.getFileName().toString().replace(".csv", "");

    Path zipPath = sourcePath.getParent().resolve(baseName + ".zip");

    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {

      if (Files.isDirectory(sourcePath)) {

        Files.walk(sourcePath)
            .filter(Files::isRegularFile)
            .forEach(
                path -> {
                  try {
                    Path relative = sourcePath.relativize(path);
                    ZipEntry entry = new ZipEntry(sourcePath.getFileName() + "/" + relative);

                    zos.putNextEntry(entry);
                    Files.copy(path, zos);
                    zos.closeEntry();

                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

      } else {
        zos.putNextEntry(new ZipEntry(sourcePath.getFileName().toString()));
        Files.copy(sourcePath, zos);
        zos.closeEntry();
      }
    }

    return zipPath;
  }
}
