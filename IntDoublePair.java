package finalProject;
import java.io.*;

import org.apache.hadoop.io.WritableComparable;

public class IntDoublePair implements WritableComparable<IntDoublePair> {

  Integer leftValue;
  Double rightValue;
  
  
  public IntDoublePair() {}
  
  //constructor
  public IntDoublePair(Integer leftValue, Double rightValue) {
	super();
	this.leftValue = leftValue;
	this.rightValue = rightValue;
  }
  
  //write pair object
  public void write(DataOutput out) throws IOException{
	  out.writeInt(leftValue);
	  out.writeDouble(rightValue);
  }
  
  //read pair object
  public void readFields(DataInput in) throws IOException{
	  leftValue = in.readInt();
	  rightValue = in.readDouble();
  }
 
  //make rules to compare two pairs: compare leftValue first and then the rightValue part
  public int compareTo(IntDoublePair other) {
	    int result = 0;
	    
	    if (equals(other)){
	    	result = 0;
	    }
	    else{
	    	int value_left = leftValue.compareTo(other.leftValue);
	    	int value_right = rightValue.compareTo(other.rightValue);
	    	if (value_left > 0){ result = 1; }
	    	if (value_left < 0){ result = -1; }
	    	if (value_left == 0){
	    		if (value_right > 0){ result = 1; }
	        	if (value_right < 0){ result = -1; }
	    	}
	    }
	    return result;
  }

  //override hashcode() to generate the hashcode of the pair
  @Override
  public int hashCode() {
	final int prime = 12;
	int result = 1;
	result = prime * result + ((leftValue == null) ? 0 : leftValue.hashCode());
	result = prime * result + ((rightValue == null) ? 0 : rightValue.hashCode());
	return result;
  }

  //override equals() to check if two pairs are the same pair
  @Override
  public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	IntDoublePair other = (IntDoublePair) obj;
	if (leftValue == null) {
		if (other.leftValue != null)
			return false;
	} else if (!leftValue.equals(other.leftValue))
		return false;
	if (rightValue == null) {
		if (other.rightValue != null)
			return false;
	} else if (!rightValue.equals(other.rightValue))
		return false;
	return true;
  }
  
  //serialize the pair object
  @Override
  public String toString() {
  	return "IntDoublePair [leftValue=" + leftValue + ", rightValue=" + rightValue + "]";
  }
  
  //get leftValue part of the pair
  public Integer getLeft() {
	return leftValue;
  }

  //get rightValue part of the pair
  public Double getRight() {
	return rightValue;
  }
}
