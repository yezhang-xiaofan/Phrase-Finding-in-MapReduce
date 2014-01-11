import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class WordSizeReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		long wordSize = 0;
		for(Text token:value){					
			wordSize++;
		}
		context.write(new Text("wordSize"), new Text(new Long(wordSize).toString()));
	}
}
