import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//use the result of 6 to create "phrase	FG=count1,BG=count2,	word1FG=count3,word1BG=count4,	
//word2FG=count5,word2BG=count6,"


//input: word\t!phrase:FG,BG\tFG,BG
//output: phrase\tFG,BG\FG,BG
public class ExtractPhraseMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		String [] tokens = value.toString().split("\t");
		String []temp = tokens[1].split(":");
		String phrase = temp[0].substring(1);
		String phrasecount = temp[1];
		context.write(new Text(phrase), new Text(phrasecount+"\t"+tokens[2]));		
	}

}
