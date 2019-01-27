package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, IntPair> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	// read the line and split into words  
    String line = value.toString();
    String[] words = line.split(",");
    // extract userID, movieID and rating  
    if(words.length == 3){
    	int user = Integer.parseInt(words[1]);
    	int movieID = Integer.parseInt(words[0]);
    	int rating = (int) Double.parseDouble(words[2]);
    	
    	// emit user, (movieID, rating) to reducer
    	context.write(new IntWritable(user), new IntPair(movieID,rating));
    	
    }
    
  }
}