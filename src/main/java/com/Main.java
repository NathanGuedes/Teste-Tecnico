package com;

import com.support.ArchiveHandler;
import com.support.FileIOService;
import com.support.QuarterlyReportScraper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws IOException {

        String BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

        QuarterlyReportScraper client = new QuarterlyReportScraper();

        List<String> links = client.findRecentReportUrls(BASE_URL, 3);

        ArchiveHandler fileDownloader = new ArchiveHandler(BASE_URL, links);
        fileDownloader.ArchiveDownloader();

        FileIOService fileIOService = new FileIOService();

        Path dir = Path.of(System.getProperty("user.dir"), "extract");

        List<Path> files;
        try (Stream<Path> paths = Files.list(dir)) {
            files = paths
                    .filter(Files::isRegularFile)
                    .toList();
        }

        fileIOService.filterFile(files,"DESCRICAO" ,"Despesas com Eventos/Sinistros", "");
    }
}