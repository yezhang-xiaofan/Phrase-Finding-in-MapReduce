import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//obtain the dictionary size of background bigram
public class BiSizeMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		if(tokens[1].contains("FG")){
			context.write(new Text("FgBi"), new Text(new Integer(1).toString()));
		}
		if(tokens[1].contains("BG")){
			context.write(new Text("BgBi"), new Text(new Integer(1).toString()));
		}
	}

}
