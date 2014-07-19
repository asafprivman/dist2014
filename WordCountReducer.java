package wordcount;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int wordTotal;
    
	protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        try{
	        String [] pair = key.toString().split("\t")[1].split(" ");
	        
	        if(pair[1].matches("!")) 
	        {
	        	if(!pair[0].matches("!")) 
	        		wordTotal = sum;
	        }
	        else key.set(key.toString() + " " + Integer.toString(wordTotal));
	        context.write(key, new IntWritable(sum));
        }
        catch(java.lang.RuntimeException e){
            key.set("Reducer1 recieved a RuntimeException : " + e.getMessage() + ".");
            context.write(key,new IntWritable(-1));
        }
    }
}
