import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;

/**
 * MapFile是已经排过序的SequnceFile,它有索引，可以按键查找。
 * 写入MapFIle
 */
public class MapFileWriteDemo {
    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };
    public static void main(String[] args) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        MapFile.Writer writer = null;
        try{
            MapFile.Writer.Option keyClassOption = MapFile.Writer.keyClass(key.getClass());
            SequenceFile.Writer.Option valueClassOption = MapFile.Writer.valueClass(value.getClass());
            writer = new MapFile.Writer(conf,new Path(uri),keyClassOption,valueClassOption);
            for(int i=0;i<1024;i++){
                key.set(i+1);
                value.set(DATA[i % DATA.length]);
                writer.append(key,value);
            }
        }finally {
            IOUtils.closeStream(writer);
        }
    }
}
