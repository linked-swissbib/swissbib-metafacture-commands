package org.swissbib.linked.mf.writer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static Logger LOG = LoggerFactory.getLogger(NeoWriter.class);

    private int appendCounter = 0;
    private int totalCounter = 0;
    private int postFix = 1;
    private final StringBuilder sb = new StringBuilder();
    private final String filename;

    private final String csvDir;
    private final int csvFileLength;
    private final int batchWriteSize;


    BufferedCsvWriter(String filename, String csvDir, int csvFileLength, int batchWriteSize) {
        this.filename = filename;
        if (csvDir == null) {
            try {
                throw new IOException("No path to csv output directory indicated!");
            } catch (IOException e) {
                LOG.error("No path to csv output directory indicated!");
            }
        } else {
            if (!csvDir.endsWith("/")) {
                csvDir += "/";
            }
        }
        this.csvDir = csvDir;
        this.csvFileLength = csvFileLength;
        this.batchWriteSize = batchWriteSize;
        sb.append(newHeader());
    }

    void append(String text) {
        if (appendCounter > batchWriteSize) {
            flush();
        }
        if (totalCounter > csvFileLength) {
            flush();
            postFix++;
            totalCounter = 0;
        }
        sb.append(text);
        appendCounter++;
        totalCounter++;
    }

    private void flush() {
        try {
            Files.write(Paths.get(csvDir + filename + "_" + StringUtils.leftPad(Integer.toString(postFix), 4, '0') + ".csv"), sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
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
        if (filename.split("_")[0].length() == 4) {
            header = createRelaHeader();
        } else {
            header = createNodeHeader();
        }
        return header;
    }

    private String createNodeHeader() {
        // return "nodeId:ID(" + filename + "),subentity,name,addName,date,:LABEL\n";
        return "nodeId:ID,subentity,name,addName,date\n";
    }

    private String createRelaHeader() {
        //return ":START_ID(" + filename.split("-")[0] + "),name,:END_ID(" + filename.split("-")[1] + ")\n";
        return ":START_ID,:END_ID\n";
    }
}
