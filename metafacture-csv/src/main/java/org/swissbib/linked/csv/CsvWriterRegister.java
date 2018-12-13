package org.swissbib.linked.csv;

import java.util.HashMap;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 02.07.16
 */
public class CsvWriterRegister {

    private final HashMap<String, BufferedCsvWriter> hm = new HashMap<>();

    private final String csvDir;
    private final int csvFileLength;
    private final int batchWriteSize;

    public CsvWriterRegister(String csvDir, int csvFileLength, int batchWriteSize) {
        this.csvDir = csvDir;
        this.csvFileLength = csvFileLength;
        this.batchWriteSize = batchWriteSize;
    }

    private void register(String name, BufferedCsvWriter writer) {
        hm.put(name, writer);
    }

    public BufferedCsvWriter ask(String name) {
        BufferedCsvWriter writer;
        if (hm.containsKey(name)) {
            writer = hm.get(name);
        } else {
            writer = new BufferedCsvWriter(name, csvDir, csvFileLength, batchWriteSize);
            register(name, writer);
        }
        return writer;
    }

    public void close() {
        for (HashMap.Entry<String,BufferedCsvWriter> entry : hm.entrySet()) {
            entry.getValue().close();
        }
        }

}
