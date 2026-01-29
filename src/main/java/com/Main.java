package com;

import com.client.ApiClient;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        String BASE_URI = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/";

        ApiClient client = new ApiClient();

        List<String> files = client.getFiles(BASE_URI, 3);

        System.out.println(files);
    }
}