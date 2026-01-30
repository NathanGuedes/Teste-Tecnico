package com.support;

import java.io.*;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FileIOService {

    public void filterFile(List<Path> files, String field, String filter) throws IOException {
        File baseDir = new File(System.getProperty("user.dir"), "archives_data");
        Map<String, String> headers = new LinkedHashMap<>();

        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }

        for (Path file : files) {
            File outputFile = new File(baseDir, "filtered_" + file.getFileName().toString());

            try (BufferedReader br = new BufferedReader(new FileReader(file.toFile())); BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true))) {
                String line = br.readLine();

                if (line != null) {
                    String[] vars = line.split(";");

                    for (int i = 0; i < vars.length; i++) {
                        headers.put(vars[i].replaceAll("\"", ""), null);
                    }

                    bw.write(line);
                    bw.newLine();
                }

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(";");
                    int i = 0;
                    for (Map.Entry<String, String> entry : headers.entrySet()) {
                        if (i < values.length) {
                            if (values[i].contains("\"")){
                                values[i] = values[i].replaceAll("\"", "");
                            }
                            entry.setValue(values[i]);
                        }
                        i++;
                    }

                    String value = headers.get(field).replaceAll("\\s", "").toLowerCase();
                    filter = filter.replaceAll("\\s", "").toLowerCase();

                    if (value.equals(filter)){
                        bw.write(line);
                        bw.newLine();
                    }
                }
            }
        }
    }


}
