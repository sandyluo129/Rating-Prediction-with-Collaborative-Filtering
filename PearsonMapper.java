package finalProject;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class PearsonMapper extends Mapper<LongWritable, Text, IntPair, IntPair> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	 // parse and split the value
	 String line = value.toString();
	 line=line.replace("(", "").replace(")", "");
	 String[] data=line.split("\t"); 
	 String[] movies=data[0].split(",");
	 String[] ratings=data[1].split(",");
	 
	 // parse movie and rating
	 int movie1=Integer.parseInt(movies[0].trim());
	 int movie2=Integer.parseInt(movies[1].trim());	
	 int rating1=Integer.parseInt(ratings[0].trim());
	 int rating2=Integer.parseInt(ratings[1].trim());
	 
	 // emit pair of movie and pair of rating
	 context.write(new IntPair(movie1,movie2),new IntPair(rating1,rating2));
  }
}
