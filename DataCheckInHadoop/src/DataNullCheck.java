

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/* created by yayi.jyy
 * To check whether the column is null.
 *  input,out,the column, inputlength are needed.
 */
public class DataNullCheck {
	private static int  column=1;
	private static int len=2;
	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		
		//public void configure(JobConf job) { 
		//	String confFile = "/Users/jiangyayi/Jobs/Data/conf/"; 
//		if (!column.initialize(confFile)){
//			throw new RuntimeException( "failed to init normalize with config file [" + confFile + "]"); }
//			simpleNormalizeOption = job.getInt("simple_normalize_option", 124); 
//			resortNormalizeOption = job.getInt("resort_normalize_option", 252); 
//			catNum = job.getInt("cat_num", 10); 
		//	}
			
		
			public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
				String line = value.toString();
				if (line == null || line.isEmpty()) return;
				//String[] s=line.split(Common.CTRL_A);
				String[] s=line.split("\u0001");
				
//					if(column==len-1){//如果要判断空的恰好是输入最后一列
//					if(s.length!=len){//则长度不等于length的输出
//						context.write(new Text(line),new IntWritable(1));
//						return ;
//					}
//					}
					//如果输入长度小于指定位即全空情况
					if(s.length-1<column){
						context.write(new Text(line),new IntWritable(1));
						return;
					}
				if(s[column].equals("")||s[column]==null){
				context.write(new Text(line),new IntWritable(1));
				}
			}
		}
		
	
	public static class IntSumNullReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
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
	
	
	public static void delFile(File file){
		if(file.exists()){
			if(file.isFile()){
				file.delete();
			}else{
				File files[]=file.listFiles();
				for(int i=0;i<files.length;i++){
					delFile(files[i]);
				}
			}
			file.delete();
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
			delFile(out);
		}
		if(otherArgs.length < 2){
			System.err.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}
		if(args.length>=3){
		column=Integer.parseInt(args[2]);
		}
		if(args.length>=4){
			len=Integer.parseInt(args[3]);
		}
		Job job=new Job(conf,"DataNullCheck");
		job.setJarByClass(DataNullCheck.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(IntSumNullReducer.class);
		job.setReducerClass(IntSumNullReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}

