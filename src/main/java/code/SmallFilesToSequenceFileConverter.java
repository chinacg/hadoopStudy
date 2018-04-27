package code;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 将若干个小文件打包成顺序文件的MapReduce程序
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    static class SequenceFileMapper
            implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey;

       /* @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit)split).getPath();
            filenameKey = new Text(path.toString());
        }*/
        @Override
        public void map(NullWritable nullWritable, BytesWritable bytesWritable, OutputCollector<Text, BytesWritable> outputCollector, Reporter reporter) throws IOException {
            Path path = ((FileSplit)reporter.getInputSplit()).getPath();
            filenameKey = new Text(path.toString());
            outputCollector.collect(filenameKey,bytesWritable );
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf jobConf) {

        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        JobConf jobConf = new JobConf(getConf(),getClass());
        jobConf.setInputFormat(WholeFileInputFormat.class);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(BytesWritable.class);

        jobConf.setNumReduceTasks(0);//不需要reduce操作，设置reduce数量为0
        jobConf.setMapperClass(SequenceFileMapper.class);
        JobClient.runJob(jobConf);

        return 0;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(),args);
        System.exit(exitCode);
    }
}
