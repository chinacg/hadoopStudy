import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SequenceReadDemo {
    public static void main(String[] args) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);
        SequenceFile.Reader.Option optionPath = SequenceFile.Reader.file(path);
        SequenceFile.Reader.Option optionLength = SequenceFile.Reader.length(174);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,optionPath,optionLength);
            Writable key =(Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value =(Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
                        value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
