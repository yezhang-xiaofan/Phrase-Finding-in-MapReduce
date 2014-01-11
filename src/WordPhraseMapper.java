import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//Use the file created by 4 to create "word	!phrase:FG=count1,BG=count2"

public class WordPhraseMapper extends Mapper <LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException{
			String [] tokens = value.toString().split("\t");
			String [] wordGroup = tokens[0].split(" "); 
			context.write(new Text(wordGroup[0]), new Text("!"+tokens[0]+":"+tokens[1]));	
			context.write(new Text(wordGroup[1]), new Text("!"+tokens[0]+":"+tokens[1]));
		}
}
