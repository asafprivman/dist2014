package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper2 extends
        Mapper<LongWritable, Text, Text, IntWritable> {
	
    private Text wordPair = new Text();
    private ArrayList<String> pairInfo = new ArrayList<String>();
    private IntWritable year = new IntWritable();
    private IntWritable times = new IntWritable();
    
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	// line format: decade("200") "\t" pair words " " occurrences if first word "\t" occurrences of pair.
    	try{
    		String [] lineSplit = value.toString().split("\t");
            pairInfo = new ArrayList<String>(Arrays.asList(lineSplit[1].split(" ")));
            
            year.set(Integer.parseInt(lineSplit[0].substring(0, 3)));
            times.set(Integer.parseInt(lineSplit[2]));
            
            wordPair.set(year + "\t" + SortPairDescending());
        	context.write(wordPair, times);
        }
        catch(java.lang.RuntimeException e){
        	wordPair.set("mapper2 recieved a RuntimeException : " + e.getMessage() + ".");
            context.write(wordPair,new IntWritable(-1));
        }
    }

	private String SortPairDescending() {
		String ans;
		String first = pairInfo.get(0);
		String second = pairInfo.get(1);
		
		// if the line was not a pair return as is (for example: "! !" / "word !").
		if(pairInfo.size() < 3)
			return first + " " + second;
		
		String timesOfFirst = pairInfo.get(2);
		
		if(first.compareTo(second) > 0 )
			ans=new String(first + " " + second);
		else
			ans= new String(second + " " + first);
		return ans + " " + timesOfFirst;
	}
}
