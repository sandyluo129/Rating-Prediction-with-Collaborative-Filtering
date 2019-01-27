package finalProject;

import java.io.IOException;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configurable;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

public class TopKReducer extends Reducer<IntWritable, IntDoublePair, IntWritable, Text> {
	
	// set the number of movies of recommendation
	// 10 is the default value
	private int N = 10; 
	
	@Override   
	public void reduce(IntWritable key, Iterable<IntDoublePair> values, Context context) 
	      throws IOException, InterruptedException {
		  int values_size = 0;
		  // create a topK list
		  TreeMultimap <Double,Integer> topK = TreeMultimap.create(Ordering.natural(),Ordering.natural().reverse());
		  // iterate values and put the correct movie into list
	      for (IntDoublePair val : values) {
	    	  topK.put(val.getRight(),val.getLeft());
	    	    if (topK.size() > N) {	    	    	
	                topK.remove(topK.asMap().firstKey(), topK.get(topK.asMap().firstKey()).first());
	             }
	    	    values_size += 1;
	      }
	      List<Double> keys = new ArrayList<Double>(topK.keySet());
	      int i=0 ,j=0;
	      int m = Math.min(values_size,N);
	      // iterate the topK list and emit the result with both movieID and similarity information
	      while(j < keys.size()) {
	    	  Double sim = keys.get(j);	
	          for(Integer id : topK.get(keys.get(j))) {
	        	  context.write(key,new Text(id+" "+sim));
	        	  i++;
	          }
	          j++;
	    
	      }
	      topK.clear();
	}
	   
	@Override
	protected void setup(Context context) 
			throws IOException, InterruptedException {
		// set the value of N 
		this.N = 10; 	
	}
	   
}
	   

