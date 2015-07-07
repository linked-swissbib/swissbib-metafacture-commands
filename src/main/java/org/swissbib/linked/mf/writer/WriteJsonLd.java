package org.swissbib.linked.mf.writer;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.stream.sink.ConfigurableObjectWriter;
import org.culturegraph.mf.util.FileCompression;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * An Elasticsearch Bulk API compliant output.
 *
 * @author Guenter Hipler, project swissbib, Basel
 * @author Sebastian Schüpbach, project swissbib, Basel
 *
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class WriteJsonLd<T> implements ConfigurableObjectWriter<T> {

    String header = DEFAULT_HEADER;
    String footer = DEFAULT_FOOTER;
    String separator = DEFAULT_SEPARATOR;

    boolean firstObject = true;
    boolean closed;

    static final String SET_COMPRESSION_ERROR = "Cannot compress Triple store";

    Pattern bibliographicResource;
    Pattern biboDoc;
    Pattern biboDocHeader;
    Pattern bibliographicResourceHeader;
    Pattern bracketedValue;

    String jsonldString;

    String baseOutDir = "/tmp";
    String outFilePrefix = "rdfxml1Line";
    String contextFile = "";
    int fileSize = 2000;
    int numberFilesPerDirectory = 300;
    int currentSubDir = 1;
    int numberOpenedFiles = 0;
    int numberLinesWritten = 0;

    FileCompression compression = FileCompression.AUTO;
    String encoding = "UTF-8";

    BufferedWriter fout = null;


    public WriteJsonLd()
    {
        this.bibliographicResource = Pattern.compile("(\"dct:BibliographicResource\":.*?)(})(?=})");
        this.biboDoc = Pattern.compile("(\"bibo:Document\":.*?)(})(?=,\"index\")");
        this.biboDocHeader = Pattern.compile("\"index\":.*?document.*?}");
        this.bibliographicResourceHeader = Pattern.compile("\"index\":.{1,40}bibliographicResource.*?}");
        this.bracketedValue = Pattern.compile("(\"[^\"]*\"):\\[(\"[^\"]*\")\\]");
    }


    public void setBaseOutDir (final String outDir) {
        this.baseOutDir = outDir;
    }


    public void setOutFilePrefix (final String filePrefix) {
        this.outFilePrefix = filePrefix;
    }


    public void setFileSize (final int fileSize) {
        this.fileSize = fileSize;
    }


    public void setContextFile (final String contextFile) throws IOException {
        String line = "";
        try (BufferedReader reader = new BufferedReader(new FileReader(contextFile))){
            while ((line=reader.readLine()) != null) this.contextFile += line;
        }
    }


    public void setFilesPerDir(int numberFilesPerDirectory) {
        this.numberFilesPerDirectory = numberFilesPerDirectory;
    }


    @Override
    public String getEncoding() {
        return Charset.defaultCharset().toString();
    }


    @Override
    public void setEncoding(String encoding) {
        throw new UnsupportedOperationException("Cannot change encoding of Search engine");
    }


    @Override
    public FileCompression getCompression() {
        return FileCompression.NONE;
    }


    @Override
    public void setCompression(FileCompression compression) {
        throw new UnsupportedOperationException(SET_COMPRESSION_ERROR);
    }


    @Override
    public void setCompression(String compression) {
        throw new UnsupportedOperationException(SET_COMPRESSION_ERROR);
    }


    @Override
    public String getHeader() {
        return DEFAULT_HEADER;
    }


    @Override
    public void setHeader(String header) {
        this.header = header;
    }


    @Override
    public String getFooter() {
        return DEFAULT_FOOTER;
    }


    @Override
    public void setFooter(String footer) {
        this.footer = footer;
    }


    @Override
    public String getSeparator() {
        return DEFAULT_SEPARATOR;
    }


    @Override
    public void setSeparator(String separator) {
        this.separator = separator;
    }


    public void process(T obj) {

        this.jsonldString = (String) obj;

        if (firstObject) {

            this.openOutFile();
            firstObject = false;

        } else {

            bracketsToList();
            String bdh = this.convertToRootNode(this.biboDocHeader, false);
            String bd = this.convertToRootNode(this.biboDoc, true);
            String brh = this.convertToRootNode(this.bibliographicResourceHeader, false);
            String br = this.convertToRootNode(this.bibliographicResource, true);
            this.writeText(bdh + bd + brh + br);

        }

    }


    private String convertToRootNode(Pattern ptr, Boolean ctx) {
        Matcher m = ptr.matcher(this.jsonldString);
        if (m.find()) {
            if (ctx) {
                return "{" + m.group(1) + "," + this.contextFile + m.group(2) + "}\n";
            } else {
                return "{" + m.group() + "}\n";
            }
        } else {
            return null;
        }
    }


    private String bracketsToList() {
        Map<String, List<String>> doubleKeys = new HashMap<>();
        Matcher m1 = this.bracketedValue.matcher(this.jsonldString);

        while (m1.find()) {
            List<String> values = doubleKeys.get(m1.group(1));
            if (values == null) {
                values = new ArrayList<>();
                doubleKeys.put(m1.group(1), values);
            }
            values.add(m1.group(2));
        }
        String output = "";
        for (Map.Entry<String, List<String>> entry: doubleKeys.entrySet()) {
            output += entry.getKey() + ":[";
            Integer i = 0;
            for (String element : entry.getValue()) {
                if (i > 0) output += ",";
                output += element;
                i++;
            }
            output += "]";
            this.jsonldString = m1.replaceFirst(output);
        }
        this.jsonldString = this.jsonldString.replaceAll("\"[^\"]*\":\\[\"[^\"]*\"\\],?", "");
        this.jsonldString = this.jsonldString.replaceAll(",}", "}");
        return this.jsonldString;
    }


    void writeText (String text) {

        try {
            if (this.fout != null) {
                this.fout.write(text);
                this.numberLinesWritten++;
                if (this.numberLinesWritten >= this.fileSize) {
                    this.numberLinesWritten = 0;
                    this.closeOutFile();
                    this.openOutFile();
                }
            }
        } catch (IOException ioExc) {
            System.out.println(ioExc.getMessage());
        }
    }


    @Override
    public void resetStream() {
        firstObject = true;
    }


    @Override
    public void closeStream() {

        this.closeOutFile();
        closed = true;

    }


    void closeOutFile () {

        if (this.fout != null) {
            try {
                this.fout.flush();
                this.fout.close();
            } catch (IOException ioEx) {
                System.out.println("io Exception while output file should be closed");
            }
        }
    }


    void openOutFile () {

        try {

            boolean subDirexists = true;
            File subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
            if (!subDir.exists()) {

                subDirexists = subDir.mkdir();
            } else if (this.numberFilesPerDirectory <= this.numberOpenedFiles) {
                this.currentSubDir++;
                subDir = new File(this.baseOutDir + File.separator + this.currentSubDir);
                if (!subDir.exists()) {
                    subDirexists = subDir.mkdir();
                }
                this.numberOpenedFiles = 0;
            }

            Date dNow = new Date( );
            SimpleDateFormat ft =  new SimpleDateFormat("yyyyMMdd_hhmmssS");

            if (subDirexists) {
                String path = this.baseOutDir + File.separator + this.currentSubDir + File.separator +
                        this.outFilePrefix + "_" + ft.format(dNow) + ".jsonld.gz";
                final OutputStream file = new FileOutputStream(path);
                OutputStream compressor = compression.createCompressor(file, path);

                this.fout = new BufferedWriter(new OutputStreamWriter(compressor,this.encoding));

            } else {
                this.fout = null;
            }


            if (this.fout != null) {
                this.numberOpenedFiles++;
            }

            //Todo: GH: Look up Exception Handlng in Metafacture Framework
            //hint: implementation of File opener in MF
        } catch (FileNotFoundException fnfEx) {
            System.out.println("file not Found");

        } catch (UnsupportedEncodingException usEnc) {
            System.out.println("UNsupportedEnding");
        }

    }

}
