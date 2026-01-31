package com.support;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class QuarterlyReportUrlScraper {

  private static final Pattern SHORT_NAME_PATTERN = Pattern.compile("(\\d)T\\d{4}\\.zip");

  private static final Pattern LONG_NAME_PATTERN = Pattern.compile("\\d{4}_(\\d)_trimestre\\.zip");

  public List<String> fetchLatestQuarterReportUrls(String baseUrl, int maxResults)
      throws IOException {

    List<String> availableYears = fetchAvailableYearsFromIndex(baseUrl);
    int currentYear = findMostRecentYear(availableYears);

    Map<Integer, String> quarterToReportMap = new TreeMap<>(Collections.reverseOrder());

    while (quarterToReportMap.size() < maxResults) {

      if (!availableYears.contains(String.valueOf(currentYear))) {
        break;
      }

      List<String> zipFiles = fetchZipFilesFromYearPage(baseUrl + currentYear);

      collectLatestQuarterReports(zipFiles, currentYear, quarterToReportMap);

      currentYear--;
    }

    return quarterToReportMap.values().stream().limit(maxResults).toList();
  }

  private List<String> fetchAvailableYearsFromIndex(String url) throws IOException {

    Document doc = fetchDocument(url);
    List<String> years = new ArrayList<>();

    for (Element link : doc.select("a")) {
      String href = link.attr("href");

      if ("../".equals(href)) continue;

      href = href.replace("/", "");

      try {
        Integer.parseInt(href);
        years.add(href);
      } catch (NumberFormatException ignored) {
      }
    }
    return years;
  }

  private List<String> fetchZipFilesFromYearPage(String url) throws IOException {

    Document doc = fetchDocument(url);
    List<String> zipFiles = new ArrayList<>();

    for (Element link : doc.select("a")) {
      String href = link.attr("href");

      if ("../".equals(href)) continue;
      if (!href.endsWith(".zip")) continue;

      zipFiles.add(href);
    }
    return zipFiles;
  }

  private void collectLatestQuarterReports(
      List<String> zipFilenames, int year, Map<Integer, String> quarterToReportMap) {

    for (String filename : zipFilenames) {
      Integer quarter = extractQuarterFromFilename(filename);

      if (quarter == null) continue;

      quarterToReportMap.putIfAbsent(quarter, year + "/" + filename);
    }
  }

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

  private static Document fetchDocument(String url) throws IOException {
    return Jsoup.connect(url).get();
  }
}
