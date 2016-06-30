

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

import util.FileUtil;

public class TheMax {
		private static int max=9999;
		private static int n=1;
		public static class Map extends Mapper<Object,Text,Text,IntWritable>{
			public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
				String line=value.toString();
				if(line==null||line.isEmpty()){
					return;
				}
				String s[]=line.split("\u0001");
				System.out.println(s[n]);
				if(s[n] != null && Pattern.matches("[0-9]+", s[n])){
				int x=Integer.parseInt(s[n]);
				
				if(x>max){
					context.write(new Text(line), new IntWritable(x));	
				}
				}
			} 
			
		}
		
		public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
			IntWritable result=new IntWritable();
			
			public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException{
				int high=0;
				for(IntWritable val:values){
					if(val.get()>high){
						high=val.get();
					}
				}
				context.write(key, new IntWritable(high));
			}
		}
		
		
		public static void main(String args[])throws Exception{
			
			Configuration conf=new Configuration();
			String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
			
			File out=new File(otherArgs[1]);
		
			if(out.exists()){
				out.delete();
			}
			if(out.isDirectory()){
				FileUtil.delFile(out);
			}
			if(otherArgs.length!=2){
				System.err.println("Usage:wordcount <in> <out>");
				System.exit(2);
			}
			
			
			Job job=new Job(conf,"TheMax");
			job.setJarByClass(TheMax.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true)?0:1);
		}
	
}
