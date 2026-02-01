package com.support;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class QuarterlyReportUrlScraper {

  // Padrão de nome curto: 1T2024.zip
  private static final Pattern SHORT_NAME_PATTERN = Pattern.compile("(\\d)T\\d{4}\\.zip");

  // Padrão de nome longo: 2024_1_trimestre.zip
  private static final Pattern LONG_NAME_PATTERN = Pattern.compile("\\d{4}_(\\d)_trimestre\\.zip");

  // Retorna as URLs dos relatórios trimestrais mais recentes
  public List<String> fetchLatestQuarterReportUrls(String baseUrl, int maxResults)
      throws IOException {

    // Busca os anos disponíveis no índice principal
    List<String> availableYears = fetchAvailableYearsFromIndex(baseUrl);

    // Obtém o ano mais recente disponível
    int currentYear = findMostRecentYear(availableYears);

    // Mapa ordenado por trimestre em ordem decrescente
    Map<Integer, String> quarterToReportMap = new TreeMap<>(Collections.reverseOrder());

    while (quarterToReportMap.size() < maxResults) {

      // Interrompe se o ano não existir no índice
      if (!availableYears.contains(String.valueOf(currentYear))) {
        break;
      }

      // Busca os arquivos ZIP do ano atual
      List<String> zipFiles = fetchZipFilesFromYearPage(baseUrl + currentYear);

      // Coleta os relatórios trimestrais válidos
      collectLatestQuarterReports(zipFiles, currentYear, quarterToReportMap);

      // Retrocede para o ano anterior
      currentYear--;
    }

    // Retorna apenas a quantidade solicitada
    return quarterToReportMap.values().stream().limit(maxResults).toList();
  }

  // Extrai os anos disponíveis a partir da página índice
  private List<String> fetchAvailableYearsFromIndex(String url) throws IOException {

    Document doc = fetchDocument(url);
    List<String> years = new ArrayList<>();

    for (Element link : doc.select("a")) {
      String href = link.attr("href");

      // Ignora navegação para diretório pai
      if ("../".equals(href)) continue;

      // Remove barra final
      href = href.replace("/", "");

      // Adiciona apenas valores numéricos válidos
      try {
        Integer.parseInt(href);
        years.add(href);
      } catch (NumberFormatException ignored) {
      }
    }
    return years;
  }

  // Obtém os nomes dos arquivos ZIP disponíveis em um ano específico
  private List<String> fetchZipFilesFromYearPage(String url) throws IOException {

    Document doc = fetchDocument(url);
    List<String> zipFiles = new ArrayList<>();

    for (Element link : doc.select("a")) {
      String href = link.attr("href");

      // Ignora diretório pai e arquivos não ZIP
      if ("../".equals(href)) continue;
      if (!href.endsWith(".zip")) continue;

      zipFiles.add(href);
    }
    return zipFiles;
  }

  // Mapeia os trimestres encontrados para seus respectivos arquivos
  private void collectLatestQuarterReports(
      List<String> zipFilenames, int year, Map<Integer, String> quarterToReportMap) {

    for (String filename : zipFilenames) {

      // Extrai o trimestre a partir do nome do arquivo
      Integer quarter = extractQuarterFromFilename(filename);

      if (quarter == null) continue;

      // Adiciona apenas se o trimestre ainda não existir
      quarterToReportMap.putIfAbsent(quarter, year + "/" + filename);
    }
  }

  // Identifica o trimestre a partir do nome do arquivo ZIP
  private Integer extractQuarterFromFilename(String filename) {

    Matcher shortMatcher = SHORT_NAME_PATTERN.matcher(filename);
    if (shortMatcher.matches()) {
      return Integer.parseInt(shortMatcher.group(1));
    }

    Matcher longMatcher = LONG_NAME_PATTERN.matcher(filename);
    if (longMatcher.matches()) {
      return Integer.parseInt(longMatcher.group(1));
    }

    return null;
  }

  // Encontra o ano mais recente a partir da lista de anos disponíveis
  private static int findMostRecentYear(List<String> years) {
    int mostRecent = Integer.parseInt(years.get(0));

    for (String year : years) {
      int value = Integer.parseInt(year);
      if (value > mostRecent) {
        mostRecent = value;
      }
    }
    return mostRecent;
  }

  // Faz o download e parse do HTML da página
  private static Document fetchDocument(String url) throws IOException {
    return Jsoup.connect(url).get();
  }
}
