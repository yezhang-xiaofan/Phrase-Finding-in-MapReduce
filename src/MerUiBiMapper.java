import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//Merge the result of 5 and 3 to create "word	
//!phrase:FG=count1,BG=count2	wordFGcount=count3,wordBGcount=count4"


//word\tphrase:FG=count1,BG=count2  and   word\tFG=count1,BG=count2
//word\tphrase:FG=count1,BG=count2\tword:FG=count1,BG=count2
public class MerUiBiMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		String word = tokens[0];		
		context.write(new Text(word), new Text(tokens[1]));	
		//context.write(new Text(wordGroup[1]), new Text(tokens[0]+":"+tokens[1]));
	}	
}
