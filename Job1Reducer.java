package finalProject;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
 
public class Job1Reducer extends Reducer<IntWritable, IntPair, IntPair, IntPair> {
	
    @Override
	public void reduce(IntWritable key, Iterable<IntPair> values, Context context)
			throws IOException, InterruptedException {
	  
	  // make a copy of values
	  ArrayList<IntPair> valuesCopy = new ArrayList<IntPair>();
	  for (IntPair value1 : values) {
		  valuesCopy.add(new IntPair(value1.getLeft(), value1.getRight()));
	  }
	  
	  // use a for loop to check if movieID1 < movieID2, and they both rated by same user, emit these two 
	  // movies as [(movie1,movie2),(rating1,rating2)] 
	  for (IntPair value1 : valuesCopy){
		  for(IntPair value2 : valuesCopy){
			  if(value1.getLeft() < value2.getLeft()){
				  IntPair moviePair = new IntPair(value1.getLeft(),value2.getLeft());
				  IntPair ratingPair = new IntPair(value1.getRight(), value2.getRight());
				  context.write(moviePair, ratingPair);
			  }
		  }
	  }
	}
}

