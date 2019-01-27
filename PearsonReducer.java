package finalProject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class PearsonReducer extends Reducer<IntPair, IntPair, IntPair, DoubleWritable>{
	private Map<Integer, Double> movieAvg;
	
	@Override
	public void reduce(IntPair key, Iterable<IntPair> values, Context context)
			throws IOException, InterruptedException {

		double dotProduct  = 0;
		double rating1Sum=0;
		double rating2Sum=0;
		double v1Avg = movieAvg.get(key.getLeft());
		double v2Avg = movieAvg.get(key.getRight());
		// do data preprocessing for calculation of Pearson
		for (IntPair RatingPair :  values) {
			int v1=RatingPair.getLeft();
			int v2=RatingPair.getRight();
			rating1Sum += (v1 - v1Avg)*(v1 - v1Avg);
			rating2Sum += (v2 - v2Avg)*(v2 - v2Avg);
			dotProduct =dotProduct +(v1 - v1Avg)*(v2 - v2Avg);
		}
		// calculate Pearson similarity
		double similarity = dotProduct /(Math.sqrt(rating1Sum)*Math.sqrt(rating2Sum));
		context.write(key,  new DoubleWritable(similarity));
	}
	
	// get average movie rating in the stage of data preprocessing
	public void setup(Context context) throws IOException, InterruptedException{
		movieAvg = getMovieAvg(new Path("movieAvgRatings.txt"));
	}
	  
	// create a function to parse movie average rating files and return the average movie ratings of a user
	private Map<Integer, Double> getMovieAvg(Path filePath){
		Map<Integer, Double> movieAvg = new HashMap<Integer, Double>();
		BufferedReader reader = null;  
		try { 
			FileSystem fileSystem = FileSystem.get(new Configuration());
		    reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));  
		    String line = null;  
		    while ((line = reader.readLine()) != null) {  
		    	line = line.replace("(", "").replace(")", "");
		        String movie_id = line.split(",")[0];
		        String average_ratings = line.split(",")[1];
		        movieAvg.put(Integer.parseInt(movie_id),Double.parseDouble(average_ratings));
		    }  
		    reader.close();  
		} catch (IOException e) {  
		    e.printStackTrace();  
		} finally {  
		    if (reader != null) {  
		        try {  
		            reader.close();  
		        } catch (IOException e1) {  
		        }  
		    }  
		}  
		return movieAvg;
	}
}
