package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper3 extends
        Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private Text wordPair = new Text();
    private DoubleWritable times = new DoubleWritable();
    
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	// add the pmi value after the year for the reducer to receive highest pmi in each decade first.
    	try{
    		String [] lineSplit = value.toString().split("\t");
            
            times.set(Double.parseDouble(lineSplit[2]));
            double sortTimesDescending = 10000 - times.get();
            
            wordPair.set(lineSplit[0] + "\t" + sortTimesDescending + "\t" + lineSplit[1]);
        	context.write(wordPair, times);
        }
        catch(java.lang.RuntimeException e){
        	wordPair.set("mapper2 recieved a RuntimeException : " + e.getMessage() + ".");
            context.write(wordPair,new DoubleWritable(-1));
        }
    }
}
