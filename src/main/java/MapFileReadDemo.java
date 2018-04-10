import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 读取mapFile
 */
public class MapFileReadDemo {
    public static void main(String[] args) throws IOException{
        String uri = args[0];
        Configuration conf = new Configuration();
        Path path = new Path(uri);
        MapFile.Reader reader = null;
        try {
            reader = new MapFile.Reader(path,conf);
            WritableComparable key =(WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value =(Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);
            while (reader.next(key,value)){
                System.out.printf("[%s]\t%s",key,value);
            }
            Text v = new Text();
            reader.get(new IntWritable(496),v);
            System.out.println(v.toString());
        }finally {
            IOUtils.closeStream(reader);
        }
    }
}
