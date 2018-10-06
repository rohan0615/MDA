package org.apache.hadoop.examples;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Comparator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.text.DecimalFormat;

public class PageRank
{
	public static int pageNum = 10878;
	public static double beta = 0.8;
	public static class MatrixNMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text key_out = new Text();
			Text value_out = new Text();
			String line = value.toString();
			String[] line_split = line.split("	");
			
			key_out.set(line_split[0]);
			value_out.set(line_split[1]);
			context.write(key_out, value_out);
		}
	}
  
	public static class MatrixNReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		int[] outLineNum = new int[pageNum+1];
		
		public void setup( Context context) throws IOException, InterruptedException
		{
			for(int i = 0; i <= pageNum; i++)
			{
				outLineNum[i] = 0;
			}
		} 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			String key_string = key.toString();
			String values_string = values.toString();
			int[] adjMatrix = new int[pageNum+1];
			for(int i = 0; i <= pageNum; i++) // initialize
			{
				adjMatrix[i] = 0;
			}
			
			for(Text val : values)
			{
				int index = Integer.parseInt(val.toString());
				sum++;
				adjMatrix[index] = 1;
			}
			if(sum == 0)
			{
				sum = 1;
			}
				
			for(int i = 0; i <= pageNum; i++)
			{
				if(adjMatrix[i] == 1)
				{
					double tmp = 1.0/(double)sum;
					context.write(null, new Text(Integer.toString(i)+" "+key.toString()+" "+Double.toString(tmp)));
				}
				
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			int initaiP = pageNum + 1;
			double initialValue = 1.0/initaiP;
			
			for(int i = 0; i <= pageNum; i++)
			{
				context.write( null, new Text(Integer.toString(i)+" "+Integer.toString(initaiP)+" "+Double.toString(initialValue)));
			}
		}
	}
  
  
	public static class matrixAMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text key_out = new Text();
			Text value_out = new Text();
			
			String line = value.toString();
			String[] line_split = line.split(" ");
			if(Integer.parseInt(line_split[1]) == pageNum+1)
			{
				for(int i = 0; i <= pageNum; i++)
				{
					key_out.set(String.valueOf(i));
					value_out.set(value.toString());
					context.write(key_out, value_out);  
				}
			}
			else
			{
				key_out.set(line_split[0]);
				value_out.set(value.toString());
				context.write(key_out, value_out); 
			}
		}
	}
  
	public static class matrixAReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		double[] r_record = new double[pageNum+1];
		public void setup(Context context) throws IOException, InterruptedException
		{
			for(int i = 0; i <= pageNum; i++)
			{
				r_record[i] = 0.0;
			}
		}
     
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//r = A*r
			double tmp = 0.0;
			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>();
			HashMap<Integer, Double> hashR = new HashMap<Integer, Double>();
			for(Text val : values)
			{
				String line = val.toString();
				String[] line_split = line.split(" ");
				
				if(Integer.parseInt(line_split[1]) == pageNum+1)
				{
					hashR.put(Integer.parseInt(line_split[0]),Double.parseDouble(line_split[2]));
				}
				else
				{
					hashA.put(Integer.parseInt(line_split[1]),Double.parseDouble(line_split[2]));
					context.write(null, new Text(val.toString()));
				}
			}
			for(int i = 0; i <= pageNum; i++)
			{
				double a = 0.0;
				double b = 0.0;
				if(hashA.containsKey(i))
				{
					a = hashA.get(i);
				}
				
				if(hashR.containsKey(i))
				{
					b = hashR.get(i);
				}

				tmp += a*b;
			}
			tmp *= beta;
			int index = Integer.parseInt(key.toString());
			tmp += (double)(1-beta)/(double)(pageNum+1);
			r_record[index] = tmp;
		}
		
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			double tmp = 0.0;
			int page_num = pageNum+1;
			for(int i = 0; i <= pageNum; i++)
			{
				tmp += r_record[i];
			}
		
			for(int i = 0; i <= pageNum; i++)
			{
				r_record[i] += (double)(1.0-tmp)/(double)(pageNum+1);
				context.write( new Text(Integer.toString(i)+" "+Integer.toString(page_num)+" "+Double.toString(r_record[i])), null);
			}
		}
	}
  
	public static class finalMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] line_split = line.split(" ");
			
			if(Integer.parseInt(line_split[1]) == pageNum+1)
			{
				context.write(new Text(String.valueOf(1)),new Text(line_split[0]+ " "+ line_split[2]));
			}
		}
	}
  
	public static class finalReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		double[] final_out = new double[pageNum+1];
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Arrays.fill(final_out, 0);
			for (Text val : values)
			{
				String line = val.toString();
				String[] line_split = line.split(" ");
				final_out[Integer.parseInt(line_split[0])] = Double.parseDouble(line_split[1]);
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException
		 
			int[] record = new int[pageNum+1];
			Arrays.fill(record, 0);
			for(int i = 0; i <= pageNum; i++)
			{
				int page_index = 0;
				double tmp = -99999.0;
				for(int j = 0; j <= pageNum; j++)
				{
					if(final_out[j] > tmp && record[j] == 0)
					{
						page_index = j;
						tmp = final_out[j];
					}
				}
				record[page_index] = 1;
				DecimalFormat output_form = new DecimalFormat("#.######");
				String output = output_form.format(final_out[page_index]);   
				context.write(new Text(Integer.toString(page_index)), new Text(output)); 
			}
       
		}
	}
  
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length != 2) {
			System.err.println("Usage:PageRank <in> <out>");
			System.exit(2);
		}
   
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(MatrixNMapper.class);
		job.setReducerClass(MatrixNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_0"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		int loopNum = 20;
		for(int i = 0; i < loopNum; i++)
		{
			job = new Job(conf, "PageRank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(matrixAMapper.class);
			job.setReducerClass(matrixAReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"_"+Integer.toString(i)));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_"+Integer.toString(i+1)));
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.waitForCompletion(true);
		}
		job = new Job(conf, "PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(finalMapper.class);
		job.setReducerClass(finalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"_"+loopNum));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_final"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}