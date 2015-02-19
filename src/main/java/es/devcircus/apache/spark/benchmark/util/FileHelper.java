/**
 * This file is part of Apache Spark Benchmark.
 *
 * Apache Spark Benchmark is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option) any later
 * version.
 *
 * Apache Spark Benchmark is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; see the file COPYING. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package es.devcircus.apache.spark.benchmark.util;

import es.devcircus.apache.spark.benchmark.util.config.ConfigurationManager;
import java.io.*;
import java.util.*;

/**
 *
 * @author Adrian Novegil <adrian.novegil@gmail.com>
 */
public abstract class FileHelper {

    /**
     * The jar file / bin directory that contains this class
     */
    public static final String CLASS_ROOT
            = new File(FileHelper.class.getProtectionDomain()
                    .getCodeSource().getLocation().getFile()).getPath();

    /**
     * The root of the benchmark directory
     */
    public static final File ROOT_DIR = new File(
            ConfigurationManager.get("apache.benchmark.config.global.root.dir"));

    /**
     * Output result file (filled in addition to stdout results)
     */
    public static final File RESULT_FILE = new File(ROOT_DIR + "/", "results.txt");

    /**
     * Deletes the files and sub directories in a specified directory.
     *
     * @param dir an existing directory to delete its content
     */
    public static void deleteDirContent(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                deleteDirContent(file);
            }
            file.delete();
        }
    }

    /**
     * Calculates db disk space in a specified file or directory.
     *
     * @param file a file or a directory to check its disk space
     * @return the total disk space in bytes.
     */
    static long getDiskSpace(File file) {
        long size = file.length();
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                String name = f.getName();
                if (name.endsWith("old") || name.endsWith("log")
                        || name.endsWith(".odr") || name.endsWith(".odb$")) {
                    continue; // temporary/log files that can be ignored
                }
                size += getDiskSpace(f);
            }
        }
        return size;
    }

    /**
     * Gets all the JAR files in a specified directory.
     *
     * @param dir a directory of JAR (and other) files
     * @return the JAR files in that directory.
     */
    static File[] getJarFiles(File dir) {
        return dir.listFiles(
                new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.toLowerCase().endsWith(".jar");
                    }
                }
        );
    }

    /**
     * Adds the paths of specified JAR files to a list of paths.
     *
     * @param jarFiles the jar files
     * @param resultPathList list to be filled with JAR paths
     */
    static void addJarFiles(File[] jarFiles, List<String> resultPathList) {
        if (jarFiles != null) {
            for (File jarFile : jarFiles) {
                resultPathList.add(jarFile.getAbsolutePath());
            }
        }
    }

    /**
     * Writes a complete text file.
     *
     * @param text the text to be written
     * @param file the file to which to write
     */
    public static void writeTextFile(String text, File file) {
        try {
            File dir = file.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
            try (FileOutputStream out = new FileOutputStream(file)) {
                out.write(text.getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Appends a single text line to a text file.
     *
     * @param line a string to be written as a new text line
     * @param file the text file to append the line to
     */
    public static void writeTextLine(String line, File file) {
        try {
            try (PrintWriter writer = new PrintWriter(new FileWriter(file, true))) {
                writer.println(line);
            }
        } catch (IOException e) {
            System.exit(1);
        }
    }
}
