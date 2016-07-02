package org.swissbib.linked.mf.writer;

import org.apache.commons.lang.StringUtils;

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
    private int totalCounter = 0;
    private int postFix = 1;
    private StringBuilder sb = new StringBuilder();
    private String filename;


    BufferedCsvWriter(String filename) {
        this.filename = filename;
        sb.append(newHeader());
    }

    void append(String text) {
        if (appendCounter > 50) {
            flush();
        }
        if (totalCounter > 1000000) {
            flush();
            sb.append(newHeader());
            postFix++;
            totalCounter = 0;
        }
        sb.append(text);
        appendCounter++;
        totalCounter++;
    }

    private void flush() {
        try {
            Files.write(Paths.get("/home/seb/temp/csv/" + filename + StringUtils.leftPad(Integer.toString(postFix), 4, '0') + ".csv"), sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        sb.setLength(0);
        appendCounter = 0;
    }


    void close() {
        flush();
    }

    private String newHeader() {
        String header;
        if (filename.contains("-")) {
            header = createRelaHeader();
        } else {
            header = createNodeHeader();
        }
        return header;
    }

    private String createNodeHeader() {
        return "nodeId:ID(" + filename + "),subentity,name,addName,date,:LABEL\n";
    }

    private String createRelaHeader() {
        return ":START_ID(" + filename.split("-")[0] + "),name,:END_ID(" + filename.split("-")[1] + ")\n";
    }
}
