import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 *  使用FSDataInputStream的seek()方法，将文件系统中的一个文件在标准输出上显示两次.
 *  seek()方法是一个相对高开销的操作，需要慎重使用，建议用流数据来构建应用的访问
 *  模式，而非执行大量额seek()方法。
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws Exception{
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        FSDataInputStream in =null;

        try{
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in,System.out,4096,false);
            in.seek(0);// 返回文件开始位置
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
