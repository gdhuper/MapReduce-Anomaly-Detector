package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.Scanner;



public class MRmapper1  extends Mapper <LongWritable,Text,Text,IntWritable> {

	 private final static IntWritable one = new IntWritable(1); 

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// TODO: filter failed USER_LOGIN records, discard the rest

	// TODO: discard records with acct name that do NOT have ""

	// TODO: write (acctname, 1) to context

	String input = value.toString().trim();
	Scanner in = new Scanner(input);
	
	while(in.hasNextLine())
	     {
		String temp = in.nextLine();
		
		if(temp.contains("type=USER_LOGIN") && temp.contains("res=failed") && temp.contains("acct=\""))
		{
			int startIdx = temp.indexOf("acct=\"");
			int endIdx = temp.indexOf("\"", startIdx+6);
			
			String acctname = temp.substring(startIdx+5, endIdx+1);
			context.write(new Text(acctname), one);
		}	
	    }
	
	}
}
