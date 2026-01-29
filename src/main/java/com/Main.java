package com;

import com.client.ApiClient;

import java.io.IOException;
import java.net.URISyntaxException;

public class Main {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        String BASE_URI = "https://dadosabertos.ans.gov.br/FTP/PDA/";

        ApiClient client = new ApiClient();
        String html = client.requestHtml(BASE_URI);

        System.out.println(html);
    }
}