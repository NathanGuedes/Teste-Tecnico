package com;

import com.support.CsvFileProcessor;
import com.support.QuarterlyReportUrlScraper;
import com.support.ZipArchiveService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class Main {

  public static void main(String[] args) throws IOException {

    // URL base onde estão os diretórios anuais dos relatórios
    ZipArchiveService zipService = getZipArchiveService();
    zipService.downloadAndExtractArchives();

    // Serviço responsável por filtrar, juntar e deduplicar arquivos CSV
    CsvFileProcessor csvProcessor = new CsvFileProcessor();

    // Diretório onde os arquivos ZIP foram extraídos
    Path extractDir = Path.of(System.getProperty("user.dir"), "extract");

    // Lista todos os arquivos extraídos
    List<Path> extractedFiles;
    try (Stream<Path> paths = Files.list(extractDir)) {
      extractedFiles = paths.filter(Files::isRegularFile).toList();
    }

    // Filtra os CSVs pela coluna DESCRICAO com o valor desejado
    csvProcessor.filterCsvFilesByColumnValue(
        extractedFiles, "DESCRICAO", "Despesas com Eventos/Sinistros", ";");

    // Diretório onde ficaram os arquivos filtrados
    Path filteredDir = csvProcessor.getFilteredOutputDir();

    // Lista os arquivos filtrados para concatenação
    List<Path> filteredFiles;
    try (Stream<Path> paths = Files.list(filteredDir)) {
      filteredFiles = paths.filter(Files::isRegularFile).toList();
    }

    // Concatena os CSVs filtrados em um único arquivo
    csvProcessor.mergeCsvFiles(filteredFiles);

    // Diretório onde o CSV consolidado foi salvo
    Path preProcessedDir = csvProcessor.getPreProcessedOutputDir();

    // Arquivo final consolidado dos trimestres
    Path consolidatedFile = preProcessedDir.resolve("consolidated_quarters_by_description.csv");

    // Remove linhas duplicadas mantendo a primeira ocorrência
    csvProcessor.removeDuplicateLines(consolidatedFile, true);

    // Diretório extra com dados externos (ex: operadoras)
    Path extraFilesDir = Paths.get(System.getProperty("user.dir"), "extra_files");

    // Arquivo externo que também precisa ser deduplicado
    Path operatorsFile = extraFilesDir.resolve("dados operadoras.csv");

    // Remove duplicatas do arquivo de operadoras
    csvProcessor.removeDuplicateLines(operatorsFile, true);
  }

  private static ZipArchiveService getZipArchiveService() throws IOException {
    String BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

    // Descobre os N relatórios trimestrais mais recentes disponíveis no site
    QuarterlyReportUrlScraper scraper = new QuarterlyReportUrlScraper();
    List<String> recentReportLinks = scraper.fetchLatestQuarterReportUrls(BASE_URL, 3);

    // Faz o download dos ZIPs encontrados e extrai os arquivos localmente
    return new ZipArchiveService(BASE_URL, recentReportLinks);
  }
}
