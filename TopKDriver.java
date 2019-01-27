package finalProject;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopKDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
	// set job name and configuration
    Job job = new Job(getConf());
    job.setJarByClass(TopKDriver.class);
    job.setJobName("TopKDriver");
    // set input and output paths
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // set mapper and reducer class
    job.setMapperClass(TopKMapper.class);
    job.setReducerClass(TopKReducer.class);
    // set mapper output key, value type
    job.setMapOutputKeyClass(IntWritable.class);   
    job.setMapOutputValueClass(IntDoublePair.class);   
    // set output key, value type
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntDoublePair.class);
    // check the exit code
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new TopKDriver(), args);
    System.exit(exitCode);
  }
}


