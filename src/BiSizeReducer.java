import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class BiSizeReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
	
	    long forcount = 0;
	    long backcount = 0;
		for(Text token : value){
			if(key.toString().equals("FgBi")){
				forcount++;
			}
			if(key.toString().equals("BgBi")){
				backcount++;
			}
		}
		context.write(new Text("FgBi"), new Text(new Long(forcount).toString()));
		context.write(new Text("BgBi"), new Text(new Long(backcount).toString()));
	}
}
