package org.swissbib.linked.mf.writer;

import java.util.HashMap;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 02.07.16
 */
class CsvWriterRegister {
    private HashMap<String, BufferedCsvWriter> hm = new HashMap<>();

    private void register(String name, BufferedCsvWriter writer) {
        hm.put(name, writer);
    }

    BufferedCsvWriter ask(String name) {
        BufferedCsvWriter writer;
        if (hm.containsKey(name)) {
            writer = hm.get(name);
        } else {
            writer = new BufferedCsvWriter(name);
            register(name, writer);
        }
        return writer;
    }

    void close() {
        for (HashMap.Entry<String,BufferedCsvWriter> entry : hm.entrySet()) {
            entry.getValue().close();
        }
        }

}
