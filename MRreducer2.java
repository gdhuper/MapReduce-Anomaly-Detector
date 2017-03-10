package Lab1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MRreducer2  extends Reducer <Text,Text,Text,DoubleWritable> {


	public final static DoubleWritable one = new DoubleWritable(1.0);

   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	// TODO: parse out (key, values) (based on hint of cleverness mapper)

	// TODO: calculate mean_failed_login_attempts and write to context

	// TODO: calculate sigma_failed_login_attempts and write to context

	// TODO: calculate num_sigmas_for:<user> and write to context
	double sum = 0;
	HashMap<String, Double> map = new HashMap<String, Double>();
	for(Text val: values)
	{
	String value = val.toString();
	String[] valArray = value.split("_");
	String acctname = valArray[0];
	String count = valArray[1];
	double Dcount = Double.parseDouble(count);
	//IntWritable value = new IntWritable(Integer.parseInt(count);)
	if(!map.containsKey(acctname)){
		map.put(acctname, Dcount); 
		}
	sum += Dcount;;		
	}	
	double mapSize = map.size();
	double mean = sum/mapSize;
	context.write(new Text("mean_failed_login_attempts"), new DoubleWritable(mean) );
	
	//Calculating sigma failed logins
	double sumSquares = 0;
	for(String Mkey: map.keySet())
	{		
		sumSquares += Math.pow((map.get(Mkey) - mean), 2);
	
	} //end for loop		 		
	double meanSigma = Math.sqrt(sumSquares / map.size());
	context.write(new Text("sigma_failed_login_attempts"), new DoubleWritable(meanSigma));
	
	//calculating num_sigmas for each user
	//	double z = 0;
	for(String Zkey : map.keySet())
	{	
	double cnt = map.get(Zkey);
	double Z = (cnt - mean) / meanSigma;
	context.write(new Text("num_sigmas_for: " + Zkey), new DoubleWritable(Z)); 
	}
	
  }
   
}
