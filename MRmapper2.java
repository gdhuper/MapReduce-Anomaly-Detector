package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.Scanner;

public class MRmapper2  extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

	// TODO: write (key, value) pair to context (hint: need to be clever here)

	String input = value.toString().trim();
	Scanner in = new Scanner(input);
	
	while(in.hasNextLine())
	     {
		String temp = in.nextLine();
		
              /*if(temp.contains("type=USER_LOGIN") && temp.contains("res=failed") && temp.contains("acct=\""))
		{
			int startIdx = temp.indexOf("acct=\"");
			int endIdx = temp.indexOf("\"", startIdx+6);
			
			String acctname = temp.substring(startIdx+5, endIdx+1);
			context.write(new Text("acct_one"), new Text(acctname + "_1"));
		}*/
		String[] str = temp.split("\\s+");
		String acctname = str[0];
		String count = str[1];
		
		context.write(new Text("record"), new Text(acctname + "_" + count));
	    }
	
	}
}
