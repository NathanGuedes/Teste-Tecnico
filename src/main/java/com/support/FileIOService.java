package com.support;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class FileIOService {

    public void filterFile(List<Path> files, String field, String filter, String separator) throws IOException {
        Path baseDir = makeDir();

        String normalizedFilter = filter.replaceAll("\\s", "").toLowerCase();

        for (Path file : files) {
            Path outputFile = getPath(file, baseDir);

            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8);
                 BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {

                System.out.println("Iniciando filtragem: " + file.getFileName());

                Iterator<String> iterator = lines.iterator();

                if (!iterator.hasNext()) {
                    continue;
                }

                String header = iterator.next();
                List<String> columns = Arrays.stream(header.split(separator))
                        .map(col -> col.replace("\"", "").trim())
                        .toList();

                int fieldIndex = columns.indexOf(field);
                if (fieldIndex == -1) {
                    throw new IllegalArgumentException("Campo '" + field + "' não encontrado");
                }

                // Escreve o cabeçalho
                writer.write(header);
                writer.newLine();

                // Processa e filtra as linhas restantes
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] values = line.split(separator);

                    if (fieldIndex < values.length) {
                        String value = values[fieldIndex]
                                .replaceAll("\"", "")
                                .replaceAll("\\s", "")
                                .toLowerCase();

                        if (value.equals(normalizedFilter)) {
                            writer.write(line);
                            writer.newLine();
                        }
                    }
                }
            }
        }
    }

    private static Path getPath(Path file, Path baseDir) {
        return baseDir.resolve("filtered_" + file.getFileName());
    }

    private static Path makeDir() throws IOException {
        Path baseDir = Paths.get(System.getProperty("user.dir"), "archives_data");
        Files.createDirectories(baseDir);
        return baseDir;
    }


}
