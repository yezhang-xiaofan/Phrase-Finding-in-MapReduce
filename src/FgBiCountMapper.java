import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//obtain the foreground bigram and background bigram count
//Count the global count for forground phrase and background phrase
public class FgBiCountMapper  extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		if(tokens[1].equals("1960")){
			context.write(new Text("FgBiCount"), new Text(tokens[2]));		
		}
		else {
			context.write(new Text("BgBiCount"), new Text(tokens[2]));
		}
	}
	
}
