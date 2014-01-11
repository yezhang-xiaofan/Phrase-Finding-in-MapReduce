import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//input   phrase/tFG,BG/tFG,BG
public class ExtractPhraseReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {	
		ArrayList<String> wordCount = new ArrayList<String>();
		Set <String> phraseCount = new HashSet<String>();
		for(Text token:value){					
			String [] temp = token.toString().split("\t");
			wordCount.add(temp[1]);
			phraseCount.add(temp[0]);
		}
		StringBuffer phraseCount1 = new StringBuffer();
		String phrasecount = null;
		for (String str:phraseCount){
			phrasecount = str;
		}
		phraseCount1.append(phrasecount+"\t");
		for(String str:wordCount){
			phraseCount1.append(str+"\t");
		}
		context.write(key, new Text(phraseCount1.toString()));
	}	
}
