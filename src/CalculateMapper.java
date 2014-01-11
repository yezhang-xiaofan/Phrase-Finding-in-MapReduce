import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//calculate points for each phrase.
public class CalculateMapper extends Mapper <LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value,
			Context context) throws IOException, InterruptedException{
		//long wordsize = 2314;
		Long wordsize = new Long(Long.parseLong("2247703"));
		
		//long fgbisize = 2312;
		Long fgbisize = new Long(Long.parseLong("35935093"));
		
		//long bgbisize = 2733;
		Long bgbisize = new Long(Long.parseLong("38834269"));
		
		//long bgbicount = 958542;
		Long bgbicount = new Long(Long.parseLong("30589724948"));
		
		//long fgbicount = 184995;	
		Long fgbicount = new Long(Long.parseLong("8539421170"));
		
		Long wordcount = new Long(Long.parseLong("10689910135"));
		//Long wordcount = new Long(Long.parseLong("6794024113"));
	
		String [] tokens = value.toString().split("\t");
	//	if(tokens[0].equals("the apple")){
		//context.write(new Text(tokens[0]), new Text(tokens[1]+"\t"+tokens[2]));
		//double phraseness = 0.0;
		String phrasinfor = tokens[1];
		String []fgPhras = phrasinfor.split(",");
		double Bgphrase = 0.0;
		double Fgphrase = 0.0;
		
		for(String str : fgPhras){
			if(str.contains("BG")){
				Bgphrase = Double.parseDouble((str.split("="))[1]);
				//System.out.println(Bgphrase);
			}
			if(str.contains("FG")){
				Fgphrase = Double.parseDouble((str.split("="))[1]);
				//System.out.println(Fgphrase);
			}
		}
		String FirstWordInfor = tokens[2];
		//System.out.println(FirstWordInfor);
		String [] WordInfor = FirstWordInfor.split(",");
		double BgWord1 = 0.0;
		double FgWord1 = 0.0;
		for(String str : WordInfor){
			if(str.contains("BG")){
				BgWord1 = Double.parseDouble((str.split("="))[1]);
				//System.out.println(BgWord1);
			}
			if(str.contains("FG")){
				FgWord1 = Double.parseDouble((str.split("="))[1]);
				//System.out.println(FgWord1);
			}
		}		
		String SecondWordInfor = tokens[3];
		//System.out.println(SecondWordInfor);
		String [] WordInfor2 = SecondWordInfor.split(",");
		double BgWord2 = 0.0;
		double FgWord2 = 0.0;
		for(String str : WordInfor2){
			if(str.contains("BG")){
				BgWord2 = Double.parseDouble((str.split("="))[1]);
				//System.out.println(BgWord2);
			}
			if(str.contains("FG")){
				FgWord2 = Double.parseDouble((str.split("="))[1]);
				//System.out.println(FgWord2);
			}
		}
		double pfg = ((double)(Fgphrase+1))/(fgbicount+fgbisize);
		double pfgx = ((double)(FgWord1+1))/(wordcount+wordsize);
		double pfgy = ((double)(FgWord2+1))/(wordcount+wordsize);
		double pbg = ((double)(Bgphrase+1))/(bgbicount+bgbisize);
		Double phraseness = pfg*Math.log(pfg/(pfgx*pfgy));
		Double informa = pfg*Math.log(pfg/pbg);
		
		Double point = phraseness + informa;
		String result = point.toString()+"\t"+phraseness.toString()+"\t"+informa.toString();
		context.write(new Text(tokens[0]), new Text(result));		
	}	
}

