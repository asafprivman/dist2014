package wordcount;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer2 extends
        Reducer<Text, IntWritable, Text, IntWritable> {
	private int wordTotal,decTotal;
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        if(key.toString().split(" ")[2].matches("!")) 
        {
        	if(key.toString().split(" ")[1].matches("!")) decTotal = sum;
        	else wordTotal = sum;
        }
        else key.set(key.toString() + " " + Integer.toString(wordTotal));
        context.write(key, new IntWritable(sum));
        	
    }
}
