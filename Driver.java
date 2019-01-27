package finalProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver  extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		  
		  int res = ToolRunner.run(new Configuration(), new Driver(), args);

	      System.exit(res);

	  }

	public int run(String[] args) throws Exception {
		
		// get hte configuration of the job
		Configuration conf = getConf();
		
		// check the number of arguments
		if (args.length != 4) {
		    System.out.printf("Usage: <input dir > <job1output dir> <job2output dir> <job3output dir>\n");
	        System.exit(-1);
		}
		
		// create the first job object
		Job job1 = new Job(conf);
		// set job name and configuration
		job1.setJarByClass(Driver.class);
		String Job1_OutputPath = args[1];
		String Job2_OutputPath = args[2];
		job1.setJobName("co-occurence");
		// set input and output paths
		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(Job1_OutputPath));
		// set mapper and reducer class
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		// set mapper output key, value type
		job1.setMapOutputKeyClass(IntWritable.class);
	    job1.setMapOutputValueClass(IntPair.class);
	    // set output key, value type
	    job1.setOutputKeyClass(IntPair.class);
	    job1.setOutputValueClass(IntPair.class);
		
	    // create the second job object
		Job job2 = new Job(conf);
		// set input and output paths
		// job1's output file is job2's input file 
		FileInputFormat.addInputPath(job2, new Path(Job1_OutputPath));
		FileOutputFormat.setOutputPath(job2, new Path(Job2_OutputPath));
		job2.setJarByClass(Driver.class);
		// set mapper and reducer class
		job2.setJobName("pearson similiar");
		job2.setMapperClass(PearsonMapper.class);
		job2.setReducerClass(PearsonReducer.class);
		// set mapper output key, value type
		job2.setMapOutputKeyClass(IntPair.class);
	    job2.setMapOutputValueClass(IntPair.class);
	    // set output key, value type
	    job2.setOutputKeyClass(IntPair.class);
	    job2.setOutputValueClass(DoubleWritable.class);
	    
	    // create the third job object
		Job job3 = new Job(conf);
		// set input and output paths
		// job2's output file is job3's input file 
		FileInputFormat.addInputPath(job3, new Path(Job2_OutputPath));
		FileOutputFormat.setOutputPath(job3,new Path(args[3]));
		job3.setJarByClass(Driver.class);
		// set mapper and reducer class
		job3.setJobName("topKMovie");
		job3.setMapperClass(TopKMapper.class);
		job3.setReducerClass(TopKReducer.class);
		// set mapper output key, value type
	    job3.setMapOutputKeyClass(IntWritable.class);   
	    job3.setMapOutputValueClass(IntDoublePair.class);   
	    // set output key, value type
	    job3.setOutputKeyClass(IntWritable.class);
	    job3.setOutputValueClass(IntDoublePair.class);
		
		boolean job1_success = job1.waitForCompletion(true);
		// job2 waiting for job1 to be finished
		if(job1_success) {
			boolean job2_success = job2.waitForCompletion(true);
			// job3 waiting for job2 to be finished
			if(job2_success) System.exit(job3.waitForCompletion(true) ? 0 : 1);
		}
		
	   	return 0;
	}
	
}
