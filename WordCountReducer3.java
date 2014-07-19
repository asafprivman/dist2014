package wordcount;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer3 extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private int maxPairsToOutput,count;
	private String decade = "";
	
	protected void setup (Context context) throws IOException, InterruptedException {
		String numOfPairsString = context.getConfiguration().get("NumOfPairs","3");
		maxPairsToOutput = Integer.parseInt(numOfPairsString);
	}
    
	protected void reduce(Text key, Iterable<DoubleWritable> values,
            Context context) throws IOException, InterruptedException {
		double firstValue = -1;
		
        try{
        	for (DoubleWritable value : values) {
    			if (firstValue != -1)
    				throw new RuntimeException("Error in Reducer3: input values contained more than one value.");
    			firstValue = value.get();
            }
        	
        	String [] keySplit = key.toString().split("\t");
        	String year = keySplit[0];
        	
    		
        	// every new decade restart the count to 0
        	if(!keySplit[0].equals(decade)) {
        		decade = keySplit[0];
        		count = 0;
        	}
        	
        	// stop after reaching max pairs to output
        	if(count<maxPairsToOutput)
        	{
        		key.set(decade + "\t" + keySplit[2]);
        		context.write(key, new DoubleWritable(firstValue));
        		count++;
        	}
        }
        catch(java.lang.RuntimeException e){
            key.set("Reducer1 recieved a RuntimeException : " + e.getMessage() + ".");
            context.write(key,new DoubleWritable(-1));
        }
    }
}
