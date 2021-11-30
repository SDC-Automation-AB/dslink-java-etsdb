package org.etsdb.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DirectoryUtils {

    public static DirectoryInfo getSize(File file) {
        return getSize(Paths.get(file.getPath()));
    }

    public static DirectoryInfo getSize(Path path) {
        DirectoryInfo info = new DirectoryInfo();
        try {
            getSizeImpl(info, path);
        } catch (IOException ignore) {
        }
        return info;
    }

    private static void getSizeImpl(DirectoryInfo info, Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            info.count += 1;
            info.size += Files.size(path);
            if (info.count % 10 == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignore) {
                }
            }
        } else {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path p : stream) {
                    getSizeImpl(info, p);
                }
            } catch (IOException ignore) {
            }
        }
    }
}
