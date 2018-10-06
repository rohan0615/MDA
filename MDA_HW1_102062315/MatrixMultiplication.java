package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class MatrixMultiplication
{
	public static int i_num = 500;
	public static int j_num = 500;
	public static int k_num = 500;
	public static class TokenizerMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text key_out = new Text();
			Text value_out = new Text();
			
			int i;
			String line = value.toString();
			String[] line_split = line.split(",");
			if(line_split[0].equals("M"))
			{
				for (i = 0; i < k_num; i++)
				{
					key_out.set(line_split[1] + "," + i);
					value_out.set(line_split[0] + "," + line_split[2] + "," + line_split[3]);
					context.write(key_out, value_out);
				}
			}
			else if(line_split[0].equals("N"))
			{
				for(i = 0; i < i_num; i++)
				{
					key_out.set(i + "," + line_split[2]);
					value_out.set(line_split[0] + "," + line_split[1] + "," + line_split[3]);
					context.write(key_out, value_out);
				}
			}
		}
	}
	public static class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			HashMap< Integer, Integer> hashM = new HashMap< Integer, Integer>();
			HashMap< Integer, Integer> hashN = new HashMap< Integer, Integer>();
			int total = 0;
			for(Text val : values)
			{
				String line = val.toString();
				String[] line_split = line.split(",");
				if(line_split[0].equals("M"))
				{
					hashM.put(Integer.parseInt(line_split[1]),Integer.parseInt(line_split[2]));
				}
				else if(line_split[0].equals("N"))
				{
					hashN.put(Integer.parseInt(line_split[1]),Integer.parseInt(line_split[2]));
				}
			}
			for(int i = 0; i < j_num; i++)
			{
				int tmp_m = 0;
				int tmp_n = 0;
				if(hashM.containsKey(i))
				{
					tmp_m = hashM.get(i);
				}
				if(hashN.containsKey(i))
				{
					tmp_n =   hashN.get(i);
				}
				total += tmp_m*tmp_n;
			}

			String out = key.toString() + "," + Integer.toString(total);
			Text output = new Text(out);
            context.write(null, output);
		} 
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		if (args.length != 2)
		{
			System.err.println("Usage: MatrixMultiplication <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MatrixMultiplication");
		job.setJarByClass(MatrixMultiplication.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
	

