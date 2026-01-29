package com.support;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FileDownloader {

    private String baseUrl;
    private List<String> urls;

    public FileDownloader(String baseUrl, List<String> urls) {
        this.baseUrl = baseUrl;
        this.urls = urls;
    }

    private void makeDir(String path){
        File theDir = new File(System.getProperty("user.dir") + File.separator + path);

        if (!theDir.exists()) {
            boolean result = false;

            try {
                theDir.mkdirs();
                result = true;
            } catch (SecurityException e) {
                e.printStackTrace();
            }
            if (result) {
                System.out.println("Directory created");
            }
        }

    }


    public void downloadFile() {
        makeDir("compress");
    }
}
