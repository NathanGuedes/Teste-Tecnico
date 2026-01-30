package com.support;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class FileIOService {

    private final Path archivesDir;
    private final Path concatArchiveDir;

    public FileIOService() throws IOException {
        this.archivesDir = makeDir("archives_data");
        this.concatArchiveDir = makeDir("concat_archive");
    }

    public void filterFile(List<Path> files, String field, String filter, String separator) throws IOException {

        String normalizedFilter = filter.replaceAll("\\s", "").toLowerCase();

        for (Path file : files) {
            Path outputFile = getPath(file, archivesDir, "filtered_");

            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8);
                 BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {

                Iterator<String> iterator = lines.iterator();
                if (!iterator.hasNext()) continue;

                String header = iterator.next();
                List<String> columns = Arrays.stream(header.split(separator))
                        .map(col -> col.replace("\"", "").trim())
                        .toList();

                int fieldIndex = columns.indexOf(field);
                if (fieldIndex == -1) {
                    throw new IllegalArgumentException("Campo '" + field + "' não encontrado");
                }

                writer.write(header);
                writer.newLine();

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] values = line.split(separator);

                    if (fieldIndex < values.length) {
                        String value = values[fieldIndex]
                                .replace("\"", "")
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

    public void concatCsvFiles(List<Path> files) throws IOException {

        Path outputFile = concatArchiveDir.resolve(
                "consolidated_quarters_by_description.csv"
        );

        boolean isFirstFile = true;

        try (BufferedWriter writer = Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {

            for (Path file : files) {
                try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {

                    Iterator<String> it = lines.iterator();
                    if (!it.hasNext()) continue;

                    String header = it.next();

                    if (isFirstFile) {
                        writer.write(header);
                        writer.newLine();
                        isFirstFile = false;
                    }

                    it.forEachRemaining(line -> {
                        try {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                }
            }
        }

    }

    private static Path getPath(Path file, Path baseDir, String prefix) {
        return baseDir.resolve(prefix + file.getFileName());
    }

    private static Path makeDir(String folderName) throws IOException {
        Path baseDir = Paths.get(System.getProperty("user.dir"), folderName);
        Files.createDirectories(baseDir);
        return baseDir;
    }
}
