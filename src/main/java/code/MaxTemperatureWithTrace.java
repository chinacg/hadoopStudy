package code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 设置HPROF分析工具来控制分析过程
 */
public class MaxTemperatureWithTrace extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length !=2){
            System.err.println("usage: MaxTemperature<input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = getConf();
        conf.setBoolean("mapreduce.task.profile",true);//启用分析工具,会在logs文件夹下对taskTracker生成profile.out文件
        conf.set("mapreduce.task.profile.params","-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
        conf.set("mapreduce.task.profile.maps","0-2");
        conf.set("mapreduce.task.profile.reduces","");  //no reduces

        Job job = Job.getInstance(conf,"Max temperature with trace");
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        return job.waitForCompletion(true)?0:1;
    }

    public  static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MaxTemperatureWithTrace(),args);
        System.exit(exitCode);
    }
}
