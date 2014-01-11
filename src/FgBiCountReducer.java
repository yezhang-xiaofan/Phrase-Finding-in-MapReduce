import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class FgBiCountReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		long BgBiCount = 0;
		for(Text token : value){
			BgBiCount += Long.parseLong(token.toString());
		}
		context.write(key, new Text(new Long(BgBiCount).toString()));
	}
}
