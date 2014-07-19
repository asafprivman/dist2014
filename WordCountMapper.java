package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text wordPair = new Text();
    private ArrayList<String> words = new ArrayList<String>();
    private IntWritable year = new IntWritable();
    private IntWritable times = new IntWritable();
    private ArrayList<String> stopWords =  new ArrayList<String>(Arrays.asList("a", "about", "above", "after"));
    
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	try {
	    	// line format: word1 + " " + word2 +..+ word5 "\t" year("2006") "\t" occurrences("7")	occurrences("7")
	        String [] lineSplit = value.toString().split("\t");
	        words = new ArrayList<String>(Arrays.asList(lineSplit[0].split(" ")));
	        
	        year.set(Integer.parseInt(lineSplit[1].substring(0, 3)));
	        times.set(Integer.parseInt(lineSplit[2]));
	        
	        RemovePunctuation();
	        AddPairsToContexts(context);
	        AddSinglesToContexts(context);
	    }
	    catch(java.lang.RuntimeException e){
	    	wordPair.set("mapper1 recieved a RuntimeException : " + e.getMessage() + ".");
	        context.write(wordPair,new IntWritable(-1));
	    }
    }

	private void RemovePunctuation() {
		for(int i=0; i<words.size(); i++){
			words.set(i, words.get(i).replaceAll("[^a-zA-Z]", ""));
			
			if(words.get(i).equalsIgnoreCase("")) {
				words.remove(i);
				i--;
			}
		}
	}
	
	private void AddPairsToContexts(Context context) throws IOException, InterruptedException{
		int middleIndex = words.size()/2;
		String middleWord = words.get(middleIndex);
		
		// Check in middle word is a stop word
		if(stopWords.contains(middleWord))
			return;
		
		for(int i=0; i<words.size(); i++){
			
			// Check if the second word is a stop word or middle word
			if(i==middleIndex || stopWords.contains(words.get(i)))
				continue;
			
			wordPair.set(year + "\t" + Sort(words.get(middleIndex),words.get(i)));
        	context.write(wordPair, times);
		}
	}
	
	private void AddSinglesToContexts(Context context) throws IOException, InterruptedException{
		int numberOfwordsAdd = 0;
		
		for(int i=0; i<words.size(); i++){
			
			// Check if word is a stop word
			if(stopWords.contains(words.get(i)))
				continue;
			
			wordPair.set(year + "\t" + words.get(i) + " !");
        	context.write(wordPair, times);
        	numberOfwordsAdd++;
		}
		
		// Add the number of words found
		wordPair.set(year + "\t" + "! !");
		IntWritable totalTimesOfWordsAdded = new IntWritable(times.get()*numberOfwordsAdd);
    	context.write(wordPair, totalTimesOfWordsAdded);
	}
	
	private String Sort(String string, String string2) {
		String ans;
		if(string.compareTo(string2) < 0 ) ans=new String(string + " " + string2);
		else ans= new String(string2 + " " + string);
		return ans;
	}
}
