import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class MerUiBiReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		//StringBuffer BiCount = new StringBuffer();
		String uniCount = null;
		ArrayList <String> biCount = new ArrayList<String>();
		for(Text token : value){
			if(token.toString().contains("!")){
				biCount.add(token.toString());
			}
			else{
				uniCount = token.toString();
			}
		}	   
		for(String str:biCount){
			StringBuffer temp = new StringBuffer();
			temp.append(str).append("\t").append(uniCount);
			context.write(key, new Text(temp.toString()));
		}
		
	}	
}
