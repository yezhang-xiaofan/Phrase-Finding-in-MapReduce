import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//obtain the dictionary size of foreground unigram
public class WordSizeMapper extends Mapper <LongWritable, Text, Text, Text>{
	
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException{
			String [] tokens = value.toString().split("\t");
			if(value.toString().contains("FG")){
				context.write(new Text("wordsize"), new Text(new Integer(1).toString()));			
			}
		}
}

