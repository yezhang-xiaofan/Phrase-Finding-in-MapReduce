import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class BiCounterReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
		long forCount = 0;
		for(Text token : value){
			String []temp = token.toString().split("\t");
			if(temp[0].equals("1960")){
				context.write(key,new Text("FG"+"="+temp[1]));
			}
			else{
				forCount += Long.parseLong(temp[1]);
			}
		}
		context.write(key, new Text("BG"+"="+new Long(forCount).toString()));		
	}
}
