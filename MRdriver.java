package Lab1; 

import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.util.Scanner;
import java.io.FileReader;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MRdriver extends Configured implements Tool {

   public int run(String[] args) throws Exception {

      // TODO: configure first MR job 

      // TODO: setup input and output paths for first MR job

      // TODO: run first MR job syncronously with verbose output set to true

      // TODO: configure the second MR job 

      // TODO: setup input and output paths for second MR job

      // TODO: run second MR job syncronously with verbose output set to true
      
      // TODO: detect anomaly based on sigma_threshold provided by user

      // TODO: for each user with score higher than threshold, print to screen:

      // detected anomaly for user: <username>  with score: <numSigmas>

	Job job = new Job(getConf(), "output1");
        job.setJarByClass(MRdriver.class);
        job.setMapperClass(MRmapper1.class);
  	   job.setReducerClass(MRreducer1.class);
       
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
       
	//job.setOutputValueClass(FloatWritable.class);
   	//job.setMapOutputValueClass(Text.class);
   	FileInputFormat.addInputPath(job, new Path(args[0]));
   	FileOutputFormat.setOutputPath(job, new Path(args[1]));
       job.waitForCompletion(true);

       Job job2 = new Job(getConf(), "output2");
        job2.setJarByClass(MRdriver.class);
        job2.setMapperClass(MRmapper2.class);
        job2.setReducerClass(MRreducer2.class);
       
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
       
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
       
        job2.setMapOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
       
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        boolean done = job2.waitForCompletion(true);


	//detecting user anomaly
	double delta = Double.parseDouble(args[3]);
	File f = new File(args[2] + "/part-r-00000");
	Scanner in = new Scanner(f);
	in.nextLine(); //skip first record
	in.nextLine();	//skip second record
	while(in.hasNextLine())
	{	
	String line = in.nextLine();
	String[] record = line.split("\\s+");
	String acctname = record[1];
	double ZScore = Double.parseDouble(record[2]);
	if (ZScore > delta)
	{
	System.out.println("detected anomaly for user: " + acctname + " with score: " + ZScore); 
	}
	
	}
	if(done) return 0;
	else return 1;

   }

   public static void main(String[] args) throws Exception { 
	   if(args.length != 4) {
		   System.err.println("usage: MRdriver <input-path> <output1-path> <output2-path> <sigma_int_threshold>");
		   System.exit(1);
	   }
	   // check sigma_int_threshold is an int
	  try {
		  Integer.parseInt(args[3]);
	  }
	  catch (NumberFormatException e) {
		  System.err.println(e.getMessage());
		  System.exit(1);
	  }
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}
