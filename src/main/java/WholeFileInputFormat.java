import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<NullWritable,BytesWritable> {

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {//不允许切片
        return false;
    }

    //运行作业的客户端通过调用getSplits() 计算分片，然后将他们发送到jobtracker,jobtracker 使用其存储位置
    //信息来调度map任务从而在tasktracker上处理这些分片数据。在tasktracker上，Map任务把输入分片传给InputFormat的
    //getRecordReader()方法来获得这个分片的RecordReader。RecordReader就像是记录上的迭代器，map任务用一个RecordReader来
    //生成记录的键/值对，然后再传递给mp函数。
    @Override
    public RecordReader<NullWritable, BytesWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
       WholeFileRecordReader reader = new WholeFileRecordReader();

        try {
            reader.initialize(inputSplit,jobConf);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reader;
    }

    class WholeFileRecordReader implements RecordReader<NullWritable,BytesWritable>{

        private FileSplit fileSplit;
        private Configuration conf;
        private BytesWritable value = new BytesWritable();
        private boolean processed = false;

        public void initialize(InputSplit split,Configuration conf) throws IOException,InterruptedException{
            this.fileSplit = (FileSplit) split;
            this.conf = conf;
        }

        @Override
        public boolean next(NullWritable nullWritable, BytesWritable bytesWritable) throws IOException {
          if(!processed){
              byte[] contents = new byte[(int)fileSplit.getLength()];
              Path file = fileSplit.getPath();
              FileSystem fs = file.getFileSystem(conf);
              FSDataInputStream in =null;
              try{
                  in = fs.open(file);
                  IOUtils.readFully(in,contents,0,contents.length);
                  value.set(contents,0,contents.length);
              }finally {
                  IOUtils.closeStream(in);
              }
              processed = true;
              return true;
          }
          return false;
        }

        @Override
        public NullWritable createKey() {
            return NullWritable.get();
        }

        @Override
        public BytesWritable createValue() {
            return value;
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public float getProgress() throws IOException {
            return processed?1.0f:0.0f;
        }
    }

}
