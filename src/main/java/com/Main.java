package com;

import static com.support.CsvTransformer.concatCsvFiles;

import com.support.*;
import com.support.enums.ComparisonOperators;
import com.support.enums.MathOperation;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class Main {

  public static void main(String[] args) throws IOException {

    // Inicializa o serviço de download e extração de arquivos zip
    ZipArchiveService zipService = buildZipArchiveService();
    zipService.downloadAndExtractArchives();

    // Define os diretórios usados no fluxo
    Path extractDir = Path.of(System.getProperty("user.dir"), "extract");
    Path normalizedDir = Path.of(System.getProperty("user.dir"), "normalized_files");
    Path transformedDir = Path.of(System.getProperty("user.dir"), "transformed_files");
    Path extraFilesDir = Path.of(System.getProperty("user.dir"), "extra_files");
    Path outputDir = Path.of(System.getProperty("user.dir"), "output");

    // Lista arquivos extraídos
    List<Path> extractedFiles;
    try (Stream<Path> paths = Files.list(extractDir)) {
      extractedFiles = paths.filter(Files::isRegularFile).toList();
    }

    // Normaliza cada CSV extraído
    for (Path file : extractedFiles) {
      new CsvNormalizer(file, ";")
          .normalizeHeaders() // padroniza nomes das colunas
          .normalizeNumbers() // padroniza formatação numérica
          .filterByColumnValue("DESCRICAO", "Despesas com Eventos/Sinistros") // filtra linhas
          .removeBlankLines() // remove linhas vazias
          .save(); // salva arquivo normalizado
    }

    // Lista arquivos normalizados
    List<Path> normalizedFiles;
    try (Stream<Path> paths = Files.list(normalizedDir)) {
      normalizedFiles = paths.filter(Files::isRegularFile).toList();
    }

    // Concatena arquivos normalizados em um único CSV
    Path mergedFile = Path.of("transformed_files/merged_dados.csv");
    concatCsvFiles(normalizedFiles, ";").save(mergedFile);

    // Define arquivo extra a ser mesclado
    Path extraFile = extraFilesDir.resolve("dados operadoras.csv");

    // Define arquivo de saída transformado
    Path transformedOutput = Path.of("transformed_files/dados.csv");

    // Aplica transformações finais no CSV
    new CsvTransformer(mergedFile, ";")
        .calculateNewColumn(
            "VL_SALDO_FINAL",
            "VL_SALDO_INICIAL",
            "VALOR_DESPESAS",
            MathOperation.SUBTRACT) // calcula saldo final
        .addYearAndQuarterColumns("DATA") // adiciona colunas de ano e trimestre
        .mergeByKey(extraFile, "REG_ANS", "REGISTRO_OPERADORA") // mescla informações adicionais
        .save(transformedOutput);

    // Valida e filtra CSV final
    new CsvValidator(transformedDir.resolve("dados.csv"), ";")
        .filterRowsByNumericValue(
            "VALOR_DESPESAS", 0, ComparisonOperators.LE) // remove despesas <= 0
        .validateCnpj("CNPJ")
        .validateRequiredField("RAZAO_SOCIAL")
        .saveFormatted("consolidado_despesas.csv"); // salva no diretório output

    new CsvTransformer(outputDir.resolve("consolidado_despesas.csv"), ";")
        .extractColumns(
            List.of(
                "DATA",
                "CNPJ",
                "RAZAO_SOCIAL",
                "DESCRICAO",
                "TRIMESTRE",
                "ANO",
                "VL_SALDO_INICIAL",
                "VL_SALDO_FINAL",
                "VALOR_DESPESAS",
                "REG_ANS",
                "MODALIDADE",
                "UF",
                "CNPJ_VALIDO",
                "OBSERVACAO"))
        .save(outputDir.resolve("consolidado_despesas.csv"));

    // Compacta o CSV final
    Helpers.zipFiles(outputDir.resolve("consolidado_despesas.csv"));

    // Limpa diretórios temporários
    Helpers.deleteDirectoryFromProjectRoot("normalized_files");
    Helpers.deleteDirectoryFromProjectRoot("transformed_files");
  }

  // Constrói o serviço de download de ZIPs a partir da URL base
  private static ZipArchiveService buildZipArchiveService() throws IOException {
    String baseUrl = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

    QuarterlyReportUrlScraper scraper = new QuarterlyReportUrlScraper();
    List<String> reports =
        scraper.fetchLatestQuarterReportUrls(baseUrl, 3); // pega últimos 3 trimestres

    return new ZipArchiveService(baseUrl, reports);
  }
}
