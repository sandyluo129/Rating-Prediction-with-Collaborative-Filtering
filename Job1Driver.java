package finalProject;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class Job1Driver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// check the number of arguments
		if (args.length != 2) {
		      System.out.printf("Usage: Job1Driver <input dir> <output dir>\n");
		      return -1;
		    }
			// set job name and configuration
		    Job job = new Job(getConf());
		    job.setJarByClass(Job1Driver.class);
		    job.setJobName("Job1");
		    // set input and output paths
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    // set mapper and reducer class
		    job.setMapperClass(Job1Mapper.class);
		    job.setReducerClass(Job1Reducer.class);
		    // set mapper output key, value type
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(IntPair.class);
		    // set output key, value type
		    job.setOutputKeyClass(IntPair.class);
		    job.setOutputValueClass(IntPair.class);
		    // check the exit code
		    boolean success = job.waitForCompletion(true);
		    return success ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new Job1Driver(), args);
	    System.exit(exitCode);
	  }
		
}

