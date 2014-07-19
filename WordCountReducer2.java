package wordcount;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer2 extends
        Reducer<Text, IntWritable, Text, DoubleWritable> {
	private int wordTotal,decTotal;
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
    	int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        
    	try{
    		String [] line = key.toString().split("\t");
	        String [] pair = line[1].split(" ");
	        
	        if(pair[1].matches("!")) 
	        {
	        	if(pair[0].matches("!")) decTotal = sum;
	        	else wordTotal = sum;
	        }
	        else {
	        	// Calculate pmi:
	        	int firstWordCount = Integer.parseInt(pair[2]);
	        	int secondWordCount = wordTotal;
	        	int pairCount = sum;
	        	double pmi = Math.log(pairCount) + Math.log(decTotal) - Math.log(firstWordCount) - Math.log(secondWordCount);
	        	
	        	key.set(line[0] + "\t" + pair[0] + " " + pair[1]);
	        	context.write(key, new DoubleWritable(pmi));
	        }    	
	    }
        catch(java.lang.RuntimeException e){
            key.set("Reducer2 recieved a RuntimeException : " + e.getMessage() + ".");
            context.write(key,new DoubleWritable(-1));
        }
    }
}
