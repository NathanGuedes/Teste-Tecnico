package com;

import com.support.ArchiveHandler;
import com.support.FileIOService;
import com.support.QuarterlyReportScraper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class Main {
  public static void main(String[] args) throws IOException {

    String BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

    QuarterlyReportScraper client = new QuarterlyReportScraper();
    List<String> links = client.findRecentReportUrls(BASE_URL, 3);

    ArchiveHandler fileDownloader = new ArchiveHandler(BASE_URL, links);
    fileDownloader.ArchiveDownloader();

    FileIOService fileIOService = new FileIOService();

    Path extractDir = Path.of(System.getProperty("user.dir"), "extract");

    List<Path> extractedFiles;
    try (Stream<Path> paths = Files.list(extractDir)) {
      extractedFiles = paths.filter(Files::isRegularFile).toList();
    }

    fileIOService.filterFile(extractedFiles, "DESCRICAO", "Despesas com Eventos/Sinistros", ";");

    Path archivesDir = fileIOService.getArchivesDir();

    List<Path> filesToConcat;
    try (Stream<Path> paths = Files.list(archivesDir)) {
      filesToConcat = paths.filter(Files::isRegularFile).toList();
    }

    fileIOService.concatCsvFiles(filesToConcat);

    Path archivesConcatDir = fileIOService.getConcatArchiveDir();

    Path concatFile = archivesConcatDir.resolve("consolidated_quarters_by_description.csv");

    fileIOService.removeDuplicates(concatFile, true);
  }
}
