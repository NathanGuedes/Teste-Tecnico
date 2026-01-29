package com.client;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QuarterlyReportScraper {

    // Exemplo: 1T2023.zip
    private static final Pattern PATTERN_1 = Pattern.compile("(\\d)T\\d{4}\\.zip");
    // Exemplo: 2023_1_trimestre.zip
    private static final Pattern PATTERN_2 = Pattern.compile("\\d{4}_(\\d)_trimestre\\.zip");

    public List<String> findRecentReportUrls(String baseUrl, int limit) throws IOException {
        List<String> availableYears = fetchAvailableYears(baseUrl);
        int currentTargetYear = findMaxYear(availableYears);

        Map<Integer, String> foundQuarters = new TreeMap<>(Collections.reverseOrder());

        while (foundQuarters.size() < limit) {
            if (!availableYears.contains(String.valueOf(currentTargetYear))) {
                break;
            }

            List<String> zipFilenames = fetchZipFilenames(baseUrl + currentTargetYear);

            populateQuarterMap(zipFilenames, currentTargetYear, foundQuarters);

            currentTargetYear--;
        }

        return foundQuarters.values()
                .stream()
                .limit(limit)
                .toList();
    }

    public List<String> fetchAvailableYears(String url) throws IOException {
        Document doc = fetchDocument(url);
        List<String> years = new ArrayList<>();

        for (Element link : doc.select("a")) {
            String href = link.attr("href");

            if ("../".equals(href)) continue;

            href = href.replace("/", "");

            try {
                Integer.parseInt(href);
            } catch (NumberFormatException e) {
                continue;
            }

            years.add(href);
        }
        return years;
    }

    public List<String> fetchZipFilenames(String url) throws IOException {
        Document doc = fetchDocument(url);
        List<String> zips = new ArrayList<>();

        for (Element link : doc.select("a")) {
            String href = link.attr("href");

            if ("../".equals(href)) continue;
            if (!href.contains(".zip")) continue;

            zips.add(href);
        }
        return zips;
    }

    private void populateQuarterMap(List<String> zipFilenames, int year, Map<Integer, String> quartersMap) {
        for (String zipName : zipFilenames) {
            Integer quarter = parseQuarterFromFilename(zipName);

            if (quarter == null) {
                continue;
            }

            quartersMap.putIfAbsent(quarter, year + "/" + zipName);
        }
    }

    private Integer parseQuarterFromFilename(String filename) {
        Matcher shortMatcher = PATTERN_1.matcher(filename);
        if (shortMatcher.matches()) {
            return Integer.parseInt(shortMatcher.group(1));
        }

        Matcher longMatcher = PATTERN_2.matcher(filename);
        if (longMatcher.matches()) {
            return Integer.parseInt(longMatcher.group(1));
        }

        return null;
    }

    private static int findMaxYear(List<String> years) {
        int biggestYear = Integer.parseInt(years.get(0));

        for (String year : years) {
            int value = Integer.parseInt(year);
            if (value > biggestYear) {
                biggestYear = value;
            }
        }
        return biggestYear;
    }


    private static Document fetchDocument(String url) throws IOException {
        return Jsoup.connect(url).get();
    }
}