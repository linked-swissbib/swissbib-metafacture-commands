import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.io.ConfigurableObjectWriter;
import org.metafacture.io.FileCompression;

/**
 * A mock implementation of a writer to access result programmatically
 *
 * @author Sebastian Schüpbach, UB-IT, Basel
 */
@Description("Outputs an Elasticsearch Bulk API compliant file.")
@In(Object.class)
@Out(Void.class)
public class MockESWriter<T> implements ConfigurableObjectWriter<T> {

    private StringBuffer buffer = new StringBuffer();
    private boolean jsonCompliant = false;
    private int numberRecordsWritten = 0;


    void setJsonCompliant(Boolean jsonCompliant) {
        this.jsonCompliant = jsonCompliant;
    }

    @Override
    public void process(T obj) {
        buffer.append(this.writeText((String) obj));
    }

    String getBuffer() {
        return buffer.toString();
    }

    private String writeText(String text) {
        String res = "";
        this.numberRecordsWritten++;
        if (jsonCompliant) {
            if (this.numberRecordsWritten > 1) res += (",\n");
            text = text.substring(0, text.length() - 1);
        }
        if (!text.equals("{}\n")) res += (text);
        return res;
    }


    @Override
    public String getEncoding() {
        return null;
    }

    @Override
    public void setEncoding(String encoding) {

    }

    @Override
    public FileCompression getCompression() {
        return null;
    }

    @Override
    public void setCompression(FileCompression compression) {

    }

    @Override
    public void setCompression(String compression) {

    }

    @Override
    public String getHeader() {
        return null;
    }

    @Override
    public void setHeader(String header) {

    }

    @Override
    public String getFooter() {
        return null;
    }

    @Override
    public void setFooter(String footer) {

    }

    @Override
    public String getSeparator() {
        return null;
    }

    @Override
    public void setSeparator(String separator) {

    }

    @Override
    public void resetStream() {

    }

    @Override
    public void closeStream() {

    }
}
