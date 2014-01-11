import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
	public class CounterReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> value,
				Context context)
				throws IOException, InterruptedException {	
			//HashMap <String, Integer> forground = new HashMap<String,Integer>();
		   long forCount = 0;
			for(Text count : value){
		    		String [] temp = count.toString().split("\t");
		    		if(temp[0].equals("1960")){
		    			context.write(key, new Text("FG"+ "=" +temp[1]));
		    		}
		    		else{
		    			System.out.println(temp[0]);
		    			System.out.println(temp[1]);
		    		    forCount += Integer.parseInt(temp[1]);
		    		}
			}
			context.write(key, new Text("BG"+ "=" + new Long(forCount).toString()));			
		}
	}
