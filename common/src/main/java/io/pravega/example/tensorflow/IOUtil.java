package io.pravega.example.tensorflow;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Util class to read image, graphDef and label files.
 */
public final class IOUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(IOUtil.class);
    private IOUtil() {}

    public static byte[] readAllBytesOrExit(final InputStream file) {
        try {
            return IOUtils.toByteArray(file);
        } catch (IOException | NullPointerException ex) {
            LOGGER.error("Failed to read [{}]!", file);
            throw new RuntimeException("Failed to read [" + file + "]!", ex);
        }
    }

    public static List<String> readAllLinesOrExit(final InputStream file) {
        try {
            return Arrays.asList(org.apache.commons.io.IOUtils.toString(file).split("\n"));
        } catch (IOException  ex) {
            LOGGER.error("Failed to read [{}]!", file, ex.getMessage());
            throw new RuntimeException("Failed to read [" + file + "]!", ex);
        }
    }

    public static void createDirIfNotExists(final File directory) {
        if (!directory.exists()) {
            directory.mkdir();
        }
    }

    public static String getFileName(final String path) {
        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }
}