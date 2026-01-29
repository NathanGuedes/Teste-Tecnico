package com.client;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApiClient {

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
        int targetYear = getBiggestYear(getYears(url));

        List<String> zips = getZips(url + targetYear);

        Map<Integer, String> quarters = new TreeMap<>(Collections.reverseOrder());

        Pattern pattern1 = Pattern.compile("(\\d)T\\d{4}\\.zip");
        Pattern pattern2 = Pattern.compile("\\d{4}_(\\d)_trimestre\\.zip");

        for (String zip : zips) {
            Matcher m1 = pattern1.matcher(zip);
            Matcher m2 = pattern2.matcher(zip);

            if (m1.matches()) {
                int quarter = Integer.parseInt(m1.group(1));
                quarters.put(quarter, targetYear + "/" + zip);
                continue;
            }

            if (m2.matches()) {
                int quarter = Integer.parseInt(m2.group(1));
                quarters.put(quarter, targetYear + "/" + zip);
            }
        }

        return quarters.values()
                .stream()
                .limit(limit)
                .toList();
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
