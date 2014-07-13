package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper2 extends
        Mapper<LongWritable, Text, Text, IntWritable> {
    private Text wordPair = new Text();
    private String[] words = new String[5];
    private IntWritable year = new IntWritable();
    private IntWritable times = new IntWritable();
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	int i=0;
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int wordsInLine = tokenizer.countTokens() - 4;
        while (tokenizer.countTokens() > 4) words[i++] = tokenizer.nextToken();
        year.set(Integer.parseInt(tokenizer.nextToken().toString().substring(0, 3)));
        times.set(Integer.parseInt(tokenizer.nextToken().toString()));
        switch(wordsInLine){
        case 1:
        	break;
        case 2:
        	wordPair.set(year + " " + sort(words[0],words[1]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[0] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[1] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " !" + " !");
        	context.write(wordPair, times);
        case 3:
        	wordPair.set(year + " " + sort(words[0],words[1]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[1],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[0] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[2] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[1] + " !");
        	times.set(times.get()*2);
        	context.write(wordPair, times);
        	wordPair.set(year + " !" + " !");
        	context.write(wordPair, times);
        case 4:
        	wordPair.set(year + " " + sort(words[1],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[0],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[3],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[0] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[1] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[3] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[2] + " !");
        	times.set(times.get()*3);
        	context.write(wordPair, times);
        	wordPair.set(year + " !" + " !");
        	context.write(wordPair, times);
        case 5:
        	wordPair.set(year + " " + sort(words[1],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[0],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[3],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + sort(words[4],words[2]));
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[0] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[1] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[3] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[4] + " !");
        	context.write(wordPair, times);
        	wordPair.set(year + " " + words[2] + " !");
        	times.set(times.get()*4);
        	context.write(wordPair, times);
        	wordPair.set(year + " !" + " !");
        	context.write(wordPair, times);
        }
    }

	private String sort(String string, String string2) {
		String ans;
		if(string.compareTo(string2) > 0 ) ans=new String(string + " " + string2);
		else ans= new String(string2 + " " + string);
		return ans;
	}
}
