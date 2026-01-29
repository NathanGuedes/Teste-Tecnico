package com;

import com.support.QuarterlyReportScraper;

import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        String BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

        QuarterlyReportScraper client = new QuarterlyReportScraper();

        List<String> files = client.findRecentReportUrls(BASE_URL, 4);

        System.out.println(files);
    }
}