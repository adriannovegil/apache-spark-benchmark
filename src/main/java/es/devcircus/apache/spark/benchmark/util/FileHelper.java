package es.devcircus.apache.spark.benchmark.util;

import java.io.*;
import java.util.*;

/**
 * Helper static methods and constants for using the file system.
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
    public static final File ROOT_DIR = new File(CLASS_ROOT).getParentFile();

    /**
     * Parent directory of the DBMS implementations
     */
    public static final File DBMS_DIR = new File(ROOT_DIR, "db");

    /**
     * Subdirectories of all the DBMS implementations
     */
    public static final File[] DBMS_DIRS = FileHelper.DBMS_DIR.listFiles();

    /**
     * Parent directory of the JPA ORM providers
     */
    public static final File JPA_DIR = new File(ROOT_DIR, "jpa");

    /**
     * Subdirectories of all the JPA providers
     */
    public static final File[] JPA_DIRS = FileHelper.JPA_DIR.listFiles();

    /**
     * Temporary directory
     */
    public static final File TEMP_DIR = new File(ROOT_DIR, "temp");

    /**
     * Working directory (for database files in embedded mode)
     */
    public static final File WORK_DIR = new File(TEMP_DIR, "work");

    /**
     * Dynamically generated persistence.xml file
     */
    public static final File PU_XML_FILE
            = new File(new File(TEMP_DIR, "META-INF"), "persistence.xml");

    /**
     * Output result file (filled in addition to stdout results)
     */
//    static final File RESULT_FILE = new File(ROOT_DIR, "results.txt");
    public static final File RESULT_FILE = new File("/tmp/", "results.txt");

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
            FileOutputStream out = new FileOutputStream(file);
            try {
                out.write(text.getBytes());
            } finally {
                out.close();
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
            PrintWriter writer = new PrintWriter(new FileWriter(file, true));
            try {
                writer.println(line);                
            } finally {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
