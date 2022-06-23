package utils;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class CSVEncoder implements Encoder<OutputQuery> {
    private boolean first = true;
    private static final String charset = "UTF-8";

    @Override
    public void encode(OutputQuery element, OutputStream stream) throws IOException {
        if(first){
            System.out.println(first);
            stream.write(element.getCSVHeader().getBytes(StandardCharsets.UTF_8));
            first = false;
        }
        stream.write(element.toCSV().getBytes(charset));
        stream.write('\n');
    }
}
