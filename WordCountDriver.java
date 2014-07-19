package wordcount;

import java.io.IOException;
import java.util.Date;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        
        Formatter formatter = new Formatter();
        String date = formatter.format("%1$tm%1$td%1$tH%1$tM%1$tS", new Date()).toString();
        formatter.close();
        
        Configuration conf = new Configuration();
        // Step 1
        String outpathStep1 = "Out" + date + "-Step1";
        RunJob(conf, "Step1", "In", outpathStep1, WordCountMapper.class, WordCountReducer.class, IntWritable.class);

        // Step 2 
        String outpathStep2 = "Out" + date + "-Step2";
        RunJob(conf, "Step2", outpathStep1, outpathStep2, WordCountMapper2.class, WordCountReducer2.class, IntWritable.class);
        
        // Step 3
        conf.set("NumOfPairs", args[0]);
        String outpathStep3 = "Out" + date + "-Step3";
        RunJob(conf, "Step3", outpathStep2, outpathStep3, WordCountMapper3.class, WordCountReducer3.class, DoubleWritable.class);
   
    }

	private static void RunJob(Configuration conf, String jobName, String inputFile, String outputFile,
								Class mapper, Class reducer, Class outputValueClass) 
										throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, jobName);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(outputValueClass);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);

        System.out.println(job.waitForCompletion(true));
	}
}
