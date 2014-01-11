import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Use the file created by 2 to gather counts for the same phrase and create"phrase	Fg=count1,Bg=count2"
public class GatherBiMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		context.write(new Text(tokens[0]), new Text(tokens[1]));		
	}
}
