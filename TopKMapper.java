package finalProject;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<LongWritable, Text, IntWritable, IntDoublePair> {
	
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// read line and split data
			String data = value.toString();
			String[] words = data.split("\t");
			
			// parse similarity and movieIDs in a movie pair
			double similarity = Double.parseDouble(words[1]); 
			String movies = words[0];
		    movies = movies.replace(")","").replace("(","");
		    String[] moviePair = movies.split(",");
		    int movie2 = Integer.parseInt(moviePair[1]);
		    int movie1 = Integer.parseInt(moviePair[0]);
		    
		    //emit movie,(movie, similarity)
		    context.write(new IntWritable(movie1),new IntDoublePair(movie2,similarity));
		}
	   
}
