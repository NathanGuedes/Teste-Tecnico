package com.client;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApiClient {

    private static final Pattern PATTERN_1 = Pattern.compile("(\\d)T\\d{4}\\.zip");
    private static final Pattern PATTERN_2 = Pattern.compile("\\d{4}_(\\d)_trimestre\\.zip");

    public List<String> getYears(String url) throws IOException {
        Document doc = getHtml(url);

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

    public List<String> getZips(String url) throws IOException {
        Document doc = getHtml(url);

        List<String> zips = new ArrayList<>();

        for (Element link : doc.select("a")) {
            String href = link.attr("href");

            if ("../".equals(href)) continue;

            if (! href.contains(".zip")) continue;

            zips.add(href);
        }

        return zips;
    }

    public List<String> getFiles(String url, int limit) throws IOException {
        List<String> years = getYears(url);
        int targetYear = getBiggestYear(years);

        Map<Integer, String> quarters = new TreeMap<>(Collections.reverseOrder());

        while (quarters.size() < limit) {
            if (!years.contains(String.valueOf(targetYear))) {
                break;
            }

            List<String> zips = getZips(url + targetYear);

            extractQuarters(zips, targetYear, quarters);

            targetYear--;
        }

        return quarters.values()
                .stream()
                .limit(limit)
                .toList();
    }

    private void extractQuarters(
            List<String> zips,
            int year,
            Map<Integer, String> quarters
    ) {
        for (String zip : zips) {

            Integer quarter = extractQuarter(zip);
            if (quarter == null) {
                continue;
            }

            quarters.putIfAbsent(quarter, year + "/" + zip);
        }
    }

    private Integer extractQuarter(String zip) {
        Matcher m1 = PATTERN_1.matcher(zip);
        if (m1.matches()) {
            return Integer.parseInt(m1.group(1));
        }

        Matcher m2 = PATTERN_2.matcher(zip);
        if (m2.matches()) {
            return Integer.parseInt(m2.group(1));
        }

        return null;
    }


    private static Document getHtml(String url) throws IOException {
        return Jsoup.connect(url).get();
    }

    private static int getBiggestYear(List<String> years) {
        int biggestYear = Integer.parseInt(years.get(0));

        for (String year : years) {
            int value = Integer.parseInt(year);
            if (value > biggestYear){
                biggestYear = value;
            }
        }
        return biggestYear;
    }

}
