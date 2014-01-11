import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//For unigram file, create  "word	Fg=count1" and "word	Bg=count2 " where
//Fg is foreground count and Bg is background count"
public class CounterMapper extends Mapper <LongWritable, Text, Text, Text>{	
		protected void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException{
			String [] temp = value.toString().split("\t");
			String word = temp[0];
			String count = temp[1]+"\t"+temp[2];
			context.write(new Text(word), new Text(count));
		}
}


