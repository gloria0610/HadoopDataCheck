

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataLength {
	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		int column=1;
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String line=value.toString();
			if(line==null||line.isEmpty()){
				return;
			}
			String s[]=line.split("\u0001");
			if(s.length-1<column){
				context.write(new Text(line), new IntWritable(s.length-1));
			}
			
		}
	}

}
