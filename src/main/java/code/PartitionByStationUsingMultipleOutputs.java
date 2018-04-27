package code;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 用MultipleOutput类将整个数据集分区到以气象站id命名的文件
 * ========================================================
 * 考虑这样一个需求：按气象站来区分气象数据。这需要运行一个作业，
 * 作业的输出是每个气象站一个文件，此文件包含改气象站的所有数据记录。
 * 最好让集群为作业决定分区数：集群的reducer任务槽越多，作业完成就越快。
 * 这就是默认的HashPartitioner表现的如此出色的原因，因为它处理的分区数不限，
 * 并且确保每个分区都有一个很好的键组合使分区更均匀。如果使用HashPartitioner,
 * 每个分区就会包含多个气象站，因此，要实现每个气象站输出一个文件，必须安排每个
 * reducer写过个文件，由此就有了MultipleOutput.
 * MultipleOutput类可以将数据写到多个文件，这些文件的名称源于输出的键或者任意字符串。
 * 这允许每个reducer（或者只有map作业的mapper）创建多个文件。采用name-m-nnnn 形式的
 * 文件名用于map输出，name-r-nnnn形式的文件名用于reduce输出，其中name是由程序设定的任意
 * 名字，nnnn是一个指明块号的整数（从0开始）。块号保证从不同块（mapper或reducer)写的输出在
 * 相同名字情况下不会冲突。
 */
public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool{

    static class StationMapper
         extends Mapper<LongWritable,Text,Text,Text>{
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value);
            context.write(new Text(parser.getStationId()),value);
        }
    }

    static class MultipleOutputReducer
        extends Reducer<Text,Text,NullWritable,Text>{
        private MultipleOutputs<NullWritable,Text> multipleOutputs;
        private NcdcRecordParser parser = new NcdcRecordParser();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:values){
                   parser.parse(value);
                   String basePath = String.format("%s/%s/part",
                           parser.getStationId(),parser.getYear());
                   multipleOutputs.write(NullWritable.get(),value,basePath);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
           multipleOutputs.close();
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(getConf(),"PartitionByStationUsingMultipleOutputs");
        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));
        job.setMapperClass(StationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MultipleOutputReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputFormatClass(LazyOutputFormat.class);// LazyOutputFormat 可以保证指定分区第一条记录输出时才真正创建文件
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),args);
        System.exit(exitCode);
    }
}
