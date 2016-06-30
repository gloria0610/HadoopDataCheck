

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCount {
	public static class dataMapper extends Mapper<Object,Text,Text,IntWritable>{
		private final static IntWritable one=new IntWritable(1);
		private Text word=new Text();
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			StringTokenizer itr=new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				word.set(itr.nextToken());
				context.write(word,one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result=new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException,InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	
//	public static void delFile(File file){
//		if(file.exists()){
//			if(file.isFile()){
//				file.delete();
//			}else{
//				File files[]=file.listFiles();
//				for(int i=0;i<files.length;i++){
//					delFile(files[i]);
//				}
//			}
//			file.delete();
//		}
//	}
	
	public static void main(String args[])throws Exception{
		
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		
		File out=new File(otherArgs[1]);
		/*System.out.println(out.exists());
		if(out.exists()){
			out.delete();
			System.out.println(out.delete());
			System.out.println(out.exists());
		}*/
		if(out.isDirectory()){
			FileUtil.delFile(out);
		}
		if(otherArgs.length!=2){
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}
		
		
		Job job=new Job(conf,"wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(dataMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}