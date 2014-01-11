import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Count the word 
public class UniCountReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		long wordCount = 0;
		for(Text token:value){					
			wordCount += Integer.parseInt(token.toString());			
		}
		context.write(key, new Text(new Long(wordCount).toString()));
	}
}
