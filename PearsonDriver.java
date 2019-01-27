package finalProject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PearsonDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new Configuration(), new PearsonDriver (), args);
		System.exit(exitCode);
	}

	public int run (String[] args) throws Exception {
		// set job name and configuration
	    Job job = new Job(getConf());
	    job.setJarByClass(PearsonDriver.class);
	    job.setJobName("Cosine Similarity Job");
	    // set input and output paths
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    // set mapper and reducer class
	    job.setMapperClass(PearsonMapper.class);
	    job.setReducerClass(PearsonReducer.class);
	    // set mapper output key, value type
	    job.setMapOutputKeyClass(IntPair.class);
	    job.setMapOutputValueClass(IntPair.class);
	    // set output key, value type
	    job.setOutputKeyClass(IntPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    // check the exit code
	    boolean success = job.waitForCompletion(true);
	    return (success ? 0 : 1);
	}
}
