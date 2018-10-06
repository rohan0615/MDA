package org.apache.hadoop.examples;
 
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.math.*; 
import java.net.URI;

import org.apache.commons.math3.analysis.function.Add;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


 
public class KMeans 
{
	public static int data_num = 4601;// data num
	public static int feature_num = 58; // feature num
	public static int centroid_num = 10; // centroid num
	//**************Euclidean*************************
	public static class EuclideanMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		double[][] centroid = new double[centroid_num][feature_num]; // 10 centroid
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			int tmpRow= 0;
			BufferedReader br = new BufferedReader(new FileReader((new File(uri.getFragment()).toString())));
			while(br.ready() && tmpRow < 10)
			{
				String line = br.readLine().toString();
				String[] line_split = line.split(" ");
				for(int i = 0; i < line_split.length; i++)
				{
					double value = Double.parseDouble(line_split[i]);
					centroid[tmpRow][i] = value;
				}
				tmpRow++;
			}	
		} 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] line_split = line.split(" ");
				
			double output = Double.MAX_VALUE;
			int cluster_num = -1;
			for(int i = 0; i < centroid_num; i++)
			{
				double tmp = 0.0;
				for(int j = 0; j < feature_num; j++)
				{
					double dim_value = Double.parseDouble(line_split[j]); // jth dimension value 
					tmp += Math.pow(centroid[i][j] - dim_value, 2); 
				}
				if(tmp < output)
				{
					cluster_num = i;
					output = tmp;
				}
			}
			
			Text key_out = new Text(String.valueOf(cluster_num));
			Text value_out = new Text(String.valueOf(output) + " " + value);
			context.write( key_out, value_out );// cluster_num, Euclidean value1 value2 ......
			
		}
	}
	
	public static class EuclideanReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		double total_Euclidean;
		public void setup( Context context) throws IOException, InterruptedException
		{
			total_Euclidean = 0.0;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			double[] new_centroid = new double[feature_num];
			Arrays.fill(new_centroid, 0.0);
			
			double node_num = 0.0; // how many node in this cluster
			
			for(Text val : values)
			{
				//try to get total euc output
				
				String[] value_split = val.toString().split(" ");
				
				double Euclidean_distance = Double.parseDouble(value_split[0]);
				total_Euclidean += Euclidean_distance;
				
				//get new centroid
				for(int i = 0; i < feature_num; i++)
				{
					double dim_value = Double.parseDouble(value_split[i+1]); // value_split[0] = Euclidean Distance
					new_centroid[i] += dim_value;
				}
				node_num++;
			}
			
			String output = "";
			for(int i = 0; i < feature_num; i++)
			{
				new_centroid[i] = new_centroid[i]/node_num;
				if(i < feature_num-1)
				{
					output = output + String.valueOf(new_centroid[i]) + " ";
				}
				else if(i == feature_num-1)
				{
					output = output + String.valueOf(new_centroid[i]);
				}
			}
			
			context.write(null, new Text(output));
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(null, new Text(String.valueOf(total_Euclidean)));
		}
	
	}
	
	//**************Manhattan*************************
	public static class ManhattanMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		double[][] centroid = new double[centroid_num][feature_num]; // 10 centroid
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			int tmpRow= 0;
			BufferedReader br = new BufferedReader(new FileReader((new File(uri.getFragment()).toString())));
			while(br.ready() && tmpRow < 10)
			{
				String line = br.readLine().toString();
				String[] line_split = line.split(" ");
				for(int i = 0; i < line_split.length; i++)
				{
					double value = Double.parseDouble(line_split[i]);
					centroid[tmpRow][i] = value;
				}
				tmpRow++;
			}	
		} 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] line_split = line.split(" ");
				
			double output = Double.MAX_VALUE;
			int cluster_num = -1;
			for(int i = 0; i < centroid_num; i++)
			{
				double tmp = 0.0;
				for(int j = 0; j < feature_num; j++)
				{
					double dim_value = Double.parseDouble(line_split[j]); // jth dimension value 
					tmp += Math.abs(centroid[i][j] - dim_value); 
				}				
				if(tmp < output)
				{
					cluster_num = i;
					output = tmp;
				}
			}
			
			Text key_out = new Text(String.valueOf(cluster_num));
			Text value_out = new Text(String.valueOf(output) + " " + value);
			context.write( key_out, value_out );// cluster_num, Euclidean value1 value2 ......
			
		}
	}
	
	public static class ManhattanReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		double total_Manhattan;
		public void setup( Context context) throws IOException, InterruptedException
		{
			total_Manhattan = 0.0;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			double[] new_centroid = new double[feature_num];
			Arrays.fill(new_centroid, 0.0);
			
			double node_num = 0.0; // how many node in this cluster
			
			for(Text val : values)
			{
				//try to get total euc output
				
				String[] value_split = val.toString().split(" ");
				
				double Manhattan_distance = Double.parseDouble(value_split[0]);
				total_Manhattan += Manhattan_distance;
				
				//get new centroid
				for(int i = 0; i < feature_num; i++)
				{
					double dim_value = Double.parseDouble(value_split[i+1]); // value_split[0] = Euclidean Distance
					new_centroid[i] += dim_value;
				}
				node_num++;
			}
			
			String output = "";
			for(int i = 0; i < feature_num; i++)
			{
				new_centroid[i] = new_centroid[i]/node_num;
				if(i < feature_num-1)
				{
					output = output + String.valueOf(new_centroid[i]) + " ";
				}
				else if(i == feature_num-1)
				{
					output = output + String.valueOf(new_centroid[i]);
				}
			}
			
			context.write(null, new Text(output));
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(null, new Text(String.valueOf(total_Manhattan)));
		}
	
	} 
	 
	 
	 public static class getEuclideanMapper extends Mapper<Object, Text, Text, Text>
	 {
		double[][] centroid = new double[centroid_num][feature_num]; // 10 centroid
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException
		{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			int tmpRow= 0;
			BufferedReader br = new BufferedReader(new FileReader((new File(uri.getFragment()).toString())));
			while(br.ready() && tmpRow < 10)
			{
				String line = br.readLine().toString();
				String[] line_split = line.split(" ");
				for(int i = 0; i < line_split.length; i++)
				{
					double value = Double.parseDouble(line_split[i]);
					centroid[tmpRow][i] = value;
				}
				tmpRow++;
			}	
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] line_split = line.split(" "); 
			
			for(int i = 0; i < centroid_num;  i++)
			{
				for(int j = i + 1; j < centroid_num; j++)
				{
					double tmp = 0.0;
					double output = 0.0;
					for(int k = 0; k < feature_num; k++)
					{
						tmp += Math.pow(centroid[i][k]-centroid[j][k], 2); 
					}
					tmp = Math.sqrt(tmp);
					Text key_out = new Text(String.valueOf(i) + " - " + String.valueOf(j));
					Text value_out = new Text(String.valueOf(tmp));
					context.write(key_out, value_out);
				}
			}		
				
		}
	}
	public static class getEuclideanRuducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Text value_out = new Text();
			for (Text val : values)
			{
				value_out = val;
			}
			context.write(key, value_out);			
		}
	}
	 
	public static class getManhattanMapper extends Mapper<Object, Text, Text, Text>
	{
		double[][] centroid = new double[centroid_num][feature_num]; // 10 centroid
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException
		{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			int tmpRow= 0;
			BufferedReader br = new BufferedReader(new FileReader((new File(uri.getFragment()).toString())));
			while(br.ready() && tmpRow < 10)
			{
				String line = br.readLine().toString();
				String[] line_split = line.split(" ");
				for(int i = 0; i < line_split.length; i++)
				{
					double value = Double.parseDouble(line_split[i]);
					centroid[tmpRow][i] = value;
				}
				tmpRow++;
			}	
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] line_split = line.split(" "); 
			
			for(int i = 0; i < centroid_num;  i++)
			{
				for(int j = i + 1; j < centroid_num; j++)
				{
					double tmp = 0.0;
					for(int k = 0; k < feature_num; k++)
					{
						tmp += Math.abs(centroid[i][k]-centroid[j][k]); 
					}
					Text key_out = new Text(String.valueOf(i) + " - " + String.valueOf(j));
					Text value_out = new Text(String.valueOf(tmp));
					context.write(key_out, value_out);
				}
			}		
				
		}
	}
	public static class getManhattanReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Text value_out = new Text();
			for (Text val : values)
			{
				value_out = val;
			}
			context.write(key, value_out);			
		}
	} 
	 
	 
	 public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.exit(2);
		}
		int MAX_ITER = 20;
		
		//****************Euclidean + c1*****************
		Job job = Job.getInstance();
		job.addCacheFile( new URI( "data/" + otherArgs[0] +"#cetroid1"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(EuclideanMapper.class);
		job.setReducerClass(EuclideanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
		FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Euclidean_C1_output/R_1" ));
		job.waitForCompletion(true);

		for(int i = 1; i <= MAX_ITER; i++)
		{
			job.cleanupProgress();
			job = Job.getInstance();
			job.setJarByClass(KMeans.class);
			job.addCacheFile(new URI( "/user/root/output/Euclidean_C1_output/R_" +  Integer.toString(i) + "/part-r-00000#centroid" +   Integer.toString(i+1)));
			job.setMapperClass(EuclideanMapper.class);
			job.setReducerClass(EuclideanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
			FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Euclidean_C1_output/R_"+ Integer.toString(i+1)));
			job.waitForCompletion(true);
		}
		//********************************************
		//****************Euclidean Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Euclidean_C1_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getEuclideanMapper.class);
		job.setReducerClass(getEuclideanRuducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Euclidean_C1_output/EuclideanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Euclidean_C1_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getManhattanMapper.class);
		job.setReducerClass(getManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Euclidean_C1_output/ManhattanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Euclidean + c2*****************
		job = Job.getInstance();
		job.addCacheFile( new URI( "data/" + otherArgs[1] +"#cetroid1"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(EuclideanMapper.class);
		job.setReducerClass(EuclideanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
		FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Euclidean_C2_output/R_1" ));
		job.waitForCompletion(true);

		for(int i = 1; i <= MAX_ITER; i++)
		{
			job.cleanupProgress();
			job = Job.getInstance();
			job.setJarByClass(KMeans.class);
			job.addCacheFile(new URI( "/user/root/output/Euclidean_C2_output/R_" +  Integer.toString(i) + "/part-r-00000#centroid" +   Integer.toString(i+1)));
			job.setMapperClass(EuclideanMapper.class);
			job.setReducerClass(EuclideanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
			FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Euclidean_C2_output/R_"+ Integer.toString(i+1)));
			job.waitForCompletion(true);
		}
		//********************************************
		//****************Euclidean Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Euclidean_C2_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getEuclideanMapper.class);
		job.setReducerClass(getEuclideanRuducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Euclidean_C2_output/EuclideanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Euclidean_C2_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getManhattanMapper.class);
		job.setReducerClass(getManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Euclidean_C2_output/ManhattanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan + c1*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "data/" + otherArgs[0] +"#cetroid1"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(ManhattanMapper.class);
		job.setReducerClass(ManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
		FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Manhattan_C1_output/R_1" ));
		job.waitForCompletion(true);
			
		for(int i = 1; i <= MAX_ITER; i++)
		{
			job.cleanupProgress();
			job = Job.getInstance();
			job.setJarByClass(KMeans.class);
			job.addCacheFile(new URI( "/user/root/output/Manhattan_C1_output/R_" +  Integer.toString(i) + "/part-r-00000#centroid" +   Integer.toString(i+1)));
			job.setMapperClass(ManhattanMapper.class);
			job.setReducerClass(ManhattanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
			FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Manhattan_C1_output/R_"+ Integer.toString(i+1)));
			job.waitForCompletion(true);
		}
		//********************************************
		//****************Euclidean Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Manhattan_C1_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getEuclideanMapper.class);
		job.setReducerClass(getEuclideanRuducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Manhattan_C1_output/EuclideanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Manhattan_C1_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getManhattanMapper.class);
		job.setReducerClass(getManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Manhattan_C1_output/ManhattanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan + c2*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "data/" + otherArgs[1] +"#cetroid1"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(ManhattanMapper.class);
		job.setReducerClass(ManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
		FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Manhattan_C2_output/R_1" ));
		job.waitForCompletion(true);
			
		for(int i = 1; i <= MAX_ITER; i++)
		{
			job.cleanupProgress();
			job = Job.getInstance();
			job.setJarByClass(KMeans.class);
			job.addCacheFile(new URI( "/user/root/output/Manhattan_C2_output/R_" +  Integer.toString(i) + "/part-r-00000#centroid" +   Integer.toString(i+1)));
			job.setMapperClass(ManhattanMapper.class);
			job.setReducerClass(ManhattanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt")) ;
			FileOutputFormat.setOutputPath(job, new Path( "/user/root/output/Manhattan_C2_output/R_"+ Integer.toString(i+1)));
			job.waitForCompletion(true);
		}
		//********************************************
		
		//****************Euclidean Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Manhattan_C2_output/R_20/part-r-00000#EucDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getEuclideanMapper.class);
		job.setReducerClass(getEuclideanRuducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Manhattan_C2_output/EuclideanDistance"));
		job.waitForCompletion(true);
		//********************************************
		//****************Manhattan Distance*****************
		job.cleanupProgress();
		job = Job.getInstance();
		job.addCacheFile( new URI( "/user/root/output/Manhattan_C2_output/R_20/part-r-00000#ManDistance"  ));
		job.setJarByClass(KMeans.class);
		job.setMapperClass(getManhattanMapper.class);
		job.setReducerClass(getManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/root/output/Manhattan_C2_output/ManhattanDistance"));
		job.waitForCompletion(true);
		//********************************************
		System.exit(0);
	}
}
    
  
