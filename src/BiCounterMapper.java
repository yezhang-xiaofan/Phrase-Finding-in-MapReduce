import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//For bigram file, create "phrase	Fg=count1" and "word	Bg=count2"
public class BiCounterMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		context.write(new Text(tokens[0]), new Text(tokens[1]+"\t"+tokens[2]));		
	}
}
