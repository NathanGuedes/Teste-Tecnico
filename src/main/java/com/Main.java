package com;

import com.client.ApiClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        String BASE_URI = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

        ApiClient client = new ApiClient();
        List<String> years = client.getYears(BASE_URI);

        int biggestYear = getBiggestYear(years);

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