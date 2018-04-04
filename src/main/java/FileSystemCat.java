import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * 直接使用FileSystem 以标准输出格式显示Hadoop文件系统中的文件
 * 有时候根本不可能在应用中设置URLStreamHandlerFactory实例。在这种
 * 情况下，需要使用FileSystem API 来打开一个文件的输入流。
 */
public class FileSystemCat {
    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
