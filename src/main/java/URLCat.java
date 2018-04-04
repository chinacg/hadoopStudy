import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * 通过URLStreanHandler实例以标准输出方式显示Hadoop文件系统的文件
 */
public class URLCat {
    static {
        //通过FsUrlStreamHandlerFactory 实例调用URL对象的setURLStreamHandlerFactory方法。
        //每个java虚拟机只能调用一次这个方法，这个限制将导致有些情况下无法从Hadoop中读取数据。
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

    }
    public static void main(String[] args) throws Exception{
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
