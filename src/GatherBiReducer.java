import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class GatherBiReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		StringBuffer BiCount = new StringBuffer();
		for(Text token : value){
			BiCount.append(token+",");
		}
		context.write(key, new Text(BiCount.toString()));
	}
}
