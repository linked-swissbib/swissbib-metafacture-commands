package org.swissbib.linked.mf.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 02.07.16
 */
class BufferedCsvWriter {

    private int appendCounter = 0;
    private StringBuilder sb = new StringBuilder();
    private String filename;


    BufferedCsvWriter(String filename) {
        this.filename = filename;
        if (filename.contains("-")) {
            append(createRelaHeader());
        } else {
            append(createNodeHeader());
        }
    }

    void append(String text) {
        if (appendCounter > 10000) {
            flush();
            appendCounter = 0;
        }
        sb.append(text);
        appendCounter++;
    }

    private void flush() {
        try {
            Files.write(Paths.get("/home/seb/temp/csv/" + filename + ".csv"), sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        sb.setLength(0);
    }


    void close() {
        flush();
    }

    private String createNodeHeader() {
        return "nodeId:ID(" + filename + "),subentity,name,addName,date,:LABEL\n";
    }

    private String createRelaHeader() {
        return ":START_ID(" + filename.split("-")[0] + "),name,:END_ID(" + filename.split("-")[1] + ")\n";
    }
}
