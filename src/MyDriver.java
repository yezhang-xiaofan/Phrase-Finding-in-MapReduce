
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
public class MyDriver {
	public static void main(String [] args) throws Exception{
		Configuration conf = new Configuration();
		
		String unigram = args[0];	
		String bigram = args[1];
		String dir = args[2];
		ConfiguredJob job1 = new ConfiguredJob(conf, unigram, dir+"uniCount", "uniCount");
		job1.setClasses(CounterMapper.class, CounterReducer.class, null);
		job1.setMapOutputClasses(Text.class, Text.class);
		//job1.setReduceJobs(4);
		job1.run();
		
		ConfiguredJob job2 = new ConfiguredJob(conf, bigram, dir+"biCount", "biCount");
		job2.setClasses(BiCounterMapper.class, BiCounterReducer.class,null);
		job2.setMapOutputClasses(Text.class, Text.class);
		//job1.setReduceJobs(3);
		job2.run();
		
		ConfiguredJob job3 = new ConfiguredJob(conf, dir+"uniCount", dir+"GatherUni", "uniCount");
		job3.setClasses(GatherUniMapper.class, GatherUniReducer.class, null);
		job3.setMapOutputClasses(Text.class, Text.class);
		//job3.setReduceJobs(4);
		job3.run();
		
		ConfiguredJob job4 = new ConfiguredJob(conf, dir+"biCount", dir+"GatherBi", "biCount");
		job4.setClasses(GatherBiMapper.class, GatherBiReducer.class,null);
		job4.setMapOutputClasses(Text.class, Text.class);
		//job4.setReduceJobs(3);
		job4.run();
		
		ConfiguredJob job5 = new ConfiguredJob(conf, dir+"GatherBi", dir+"WordPhrase", "WordPhrase");
		job5.setClasses(WordPhraseMapper.class, WordPhraseReducer.class,null);
		job5.setMapOutputClasses(Text.class, Text.class);
		//job5.setReduceJobs(3);
		job5.run();
		
		//word\t!phrase:FG=count1,BG=count2
		ConfiguredJob job6 = new ConfiguredJob(conf, dir+"WordPhrase", dir+"MergeCount", "MergeCount");
		job6.addInput(dir+"GatherUni");
		job6.setClasses(MerUiBiMapper.class, MerUiBiReducer.class,null);
		job6.setMapOutputClasses(Text.class, Text.class);
		//job6.setReduceJobs(3);
		job6.run();
		
		ConfiguredJob job7 = new ConfiguredJob(conf, dir+"MergeCount", dir+"FinalCount", "FinalCount");
		//job7.addInput("GatherUni");
		job7.setClasses(ExtractPhraseMapper.class, ExtractPhraseReducer.class,null);
		job7.setMapOutputClasses(Text.class, Text.class);
		//job7.setReduceJobs(3);
		job7.run();
		
		ConfiguredJob job8 = new ConfiguredJob(conf, bigram, dir+"GlobalCount/BiCount", "FinalCount");
		//job7.addInput("GatherUni");
		job8.setClasses(FgBiCountMapper.class, FgBiCountReducer.class,null);
		job8.setMapOutputClasses(Text.class, Text.class);
		//job8.setReduceJobs(3);
		job8.run();
		
		ConfiguredJob job9 = new ConfiguredJob(conf, dir+"GatherUni", dir+"GlobalCount/UniSize", "FinalCount");
		//job7.addInput("GatherUni");
		job9.setClasses(WordSizeMapper.class, WordSizeReducer.class,null);
		job9.setMapOutputClasses(Text.class, Text.class);
		//job9.setReduceJobs(3);
		job9.run();
				
		ConfiguredJob job10 = new ConfiguredJob(conf, dir+"GatherBi", dir+"GlobalCount/BiSize", "FinalCount");
		//job7.addInput("GatherUni");
		job10.setClasses(BiSizeMapper.class, BiSizeReducer.class,null);
		job10.setMapOutputClasses(Text.class, Text.class);
		//job10.setReduceJobs(3);
		job10.run();
		
		ConfiguredJob job11 = new ConfiguredJob(conf, unigram, dir+"GlobalCount/UniCount", "FinalCount");
		
		job11.setClasses(UniCountMapper.class, UniCountReducer.class,null);
		job11.setMapOutputClasses(Text.class, Text.class);
		//job11.setReduceJobs(3);
		job11.run();
		
		ConfiguredJob job12 = new ConfiguredJob(conf, dir+"FinalCount", dir+"CalculateResult", "Calculate");
		
		job12.setClasses(CalculateMapper.class, CalculateReducer.class,null);
		job12.setMapOutputClasses(Text.class, Text.class);
		//job12.setReduceJobs(3);
		job12.run();		
	}
	
}

