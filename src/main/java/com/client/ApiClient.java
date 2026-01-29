package com.client;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    private static Document getHtml(String url) throws IOException {
        return Jsoup.connect(url).get();
    }


}
