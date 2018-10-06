package org.apache.hadoop.examples;
 
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.awt.*;
import javax.swing.*;
import java.math.*; 
import java.lang.*;
import java.net.URI;

import java.util.Random;
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


 
public class BFR {
   
   public static final int NUM = 3; //centroid number
   public static final double thresold = 10.0;

   public static class Initial_DS_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
   {
      ArrayList<String> x_list = new ArrayList<String>();
      ArrayList<String> y_list = new ArrayList<String>();
      
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException
      {
          String line = value.toString();
          String[] input_div = line.split("\\s+");
          x_list.add(input_div[0]);
          y_list.add(input_div[1]);
      }

      public void cleanup(Context context) throws IOException, InterruptedException
      { 
          Random ran = new Random();

          for(int i = 0; i < NUM; i++) //randomly pick centroids
          {
              int index = ran.nextInt(x_list.size());	///???????????????
              String out = x_list.get(index) + " " + y_list.get(index);
              
              double sumsq_x = (Double.parseDouble(x_list.get(index)))*(Double.parseDouble(x_list.get(index)));
              double sumsq_y = (Double.parseDouble(y_list.get(index)))*(Double.parseDouble(y_list.get(index))); 
              String summarize = out + " " +String.valueOf(sumsq_x)+ " " +String.valueOf(sumsq_y);
			  x_list.remove(index);
			  y_list.remove(index);
			  context.write(new Text(String.valueOf(1)), new Text(summarize));
          }
          for(int i = 0; i < x_list.size(); i++)
          {
              String out = x_list.get(i) + " " + y_list.get(i);
              context.write(new Text(String.valueOf(0)), new Text(out));
          }
      } 
  }

  public static class Initial_DS_Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
  {    
      ArrayList<String> centroid = new ArrayList<String>();
      ArrayList<Double> x_list = new ArrayList<Double>();
      ArrayList<Double> y_list = new ArrayList<Double>();
      double[] output_N = new double[NUM];
      double[][] output_sum = new double[2][NUM];
      double[][] output_sumsq = new double[2][NUM];
	  
	  ArrayList<Double> RS_xlist = new ArrayList<Double>();
	  ArrayList<Double> RS_ylist = new ArrayList<Double>();
	  
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
          if(key.toString().equals("1"))
          {
              for(Text val : values)
              {
                  centroid.add(val.toString());
				  //context.write(new Text("DS_Cluster"), val);
				  
              }
          }
          else
          {
              for(Text val : values)
              {
                  String[] line = val.toString().split("\\s+");
                  x_list.add(Double.parseDouble(line[0]));
                  y_list.add(Double.parseDouble(line[1]));
				  //context.write(new Text("DS_NormalPoints"), val);
              }
          }	
		  
      }

      public void cleanup(Context context) throws IOException, InterruptedException
      { 
		  boolean[] cluster_flag = new boolean[x_list.size()]; 
		  Arrays.fill(cluster_flag, true);
		  for(int i = 0; i < centroid.size(); i++)
		  {
			  String[] line = centroid.get(i).split("\\s+");
              double[] sum = new double[2];
              double[] sumsq = new double[2];

			  //parse all parameters
			  sum[0] = Double.parseDouble(line[0]);
              sum[1] = Double.parseDouble(line[1]);
              sumsq[0] = Double.parseDouble(line[2]);
              sumsq[1] = Double.parseDouble(line[3]);

			  //add centroid parameters into cluster info(initialization)
			  output_N[i] += 1;
			  output_sum[0][i] = sum[0];
			  output_sum[1][i] = sum[1];
			  output_sumsq[0][i] = sumsq[0];
			  output_sumsq[1][i] = sumsq[1];
			  //calculate the centoid coordinate
			  double[] cen = new double[2];
			  
              cen[0] = sum[0]/output_N[i];
              cen[1] = sum[1]/output_N[i];
			  
              //add points one by one
			  for(int j = 0; j < x_list.size(); j++)
			  {

				  if(cluster_flag[j] == true)
				  {
					  double[] coord = new double[2];
					  coord[0] = x_list.get(j);
					  coord[1] = y_list.get(j);
				  
					  //get mahalanobis distance
					  double mahalanobis_dist = 0.0;
                      for(int k = 0; k < 2; k++) // get mahalanobis distance of all dimensions k 
                      {
						  double sigma = sumsq[k]/output_N[i] - Math.pow(sum[k]/output_N[i], 2);
						  sigma = Math.abs(sigma);
						  sigma = Math.sqrt(sigma);
						  if(output_N[i]==1)
						  {
							  sigma = 1.0;
						  }
						  mahalanobis_dist += Math.pow( (coord[k]-cen[k])/sigma, 2 );
					  }
                      mahalanobis_dist = Math.sqrt(mahalanobis_dist);
				      if(mahalanobis_dist < thresold)
                      {
						  output_N[i] += 1;
						  output_sum[0][i] += coord[0];
						  output_sum[1][i] += coord[1];
						  output_sumsq[0][i] += Math.pow( coord[0], 2 );
						  output_sumsq[1][i] += Math.pow( coord[1], 2 );
						  cluster_flag[j] = false;
                          String out = String.valueOf(i) + " "+ String.valueOf(coord[0]) + " " + String.valueOf(coord[1]);
                          context.write( new Text("C"), new Text(out) );
                      }
				  } 
			  }
              // wrtie all data of a cluter out
              String cluster_info = String.valueOf(i) + " " +
                                    String.valueOf(output_N[i]) + " " + 
                                    String.valueOf(output_sum[0][i]) + " " + String.valueOf(output_sum[1][i]) + " " +
                                    String.valueOf(output_sumsq[0][i]) + " " + String.valueOf(output_sumsq[1][i]); 
              context.write( new Text("DS_Cluster"), new Text(cluster_info) );

		  }
		  for(int i = 0; i < x_list.size(); i++)
		  {
			  if(cluster_flag[i] == true) // points which dont belong to any cluster
			  {
				  double[] coord = new double[2];
				  coord[0] = x_list.get(i);
				  coord[1] = y_list.get(i);
				  RS_xlist.add(coord[0]);
				  RS_ylist.add(coord[1]);
			  }
		  }
		  for(int i = 0; i < RS_xlist.size(); i++)
		  {
			  String output_RS_coord = String.valueOf(RS_xlist.get(i)) + " " + String.valueOf(RS_ylist.get(i));
			  context.write( new Text("R"), new Text(output_RS_coord) );
		  }
          int index_count = 0; // initial index count
          context.write(new Text("COUNT"), new Text(String.valueOf(index_count)));
      }

  }
 
  public static class DS_Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
  {
    public void setup(Context context) throws IOException, InterruptedException 
    {
        URI[] files = context.getCacheFiles();
        URI uri = files[0];
        BufferedReader reader = new BufferedReader(new FileReader((new File(uri.getFragment()).toString())));
        String line;
        int count = 0;
        while(reader.ready()){
            line = reader.readLine().toString();
            context.write(new Text("new"), new Text(line));
        }
    }  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {  
        String line = value.toString();
        context.write(new Text("old"), new Text(line));
    }
  }

  public static class DS_Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
  {
    ArrayList<String> centroid = new ArrayList<String>();
    ArrayList<Double> x_list = new ArrayList<Double>();
    ArrayList<Double> y_list = new ArrayList<Double>();
    double[] output_N = new double[NUM];
    double[][] output_sum = new double[2][NUM];
    double[][] output_sumsq = new double[2][NUM];

    ArrayList<Double> RS_xlist = new ArrayList<Double>();
    ArrayList<Double> RS_ylist = new ArrayList<Double>();

    int index_count = 0;
    // points for kmeans
    ArrayList<Double> KmeansP_xlist = new ArrayList<Double>();
    ArrayList<Double> KmeansP_ylist = new ArrayList<Double>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        //deal with new nodes
        if(key.toString().equals("new"))
        {
            for(Text val : values)
            {
                String[] input_div = val.toString().split("\\s+");
                x_list.add(Double.parseDouble(input_div[0]));
                y_list.add(Double.parseDouble(input_div[1]));             
            }
        }
        //deal with old points : R、DS_Cluster、C、COUNT、KMeansCen
        else
        {
            for(Text val : values)
            {
                String[] input_div = val.toString().split("\\s+");
                //Deals with Initial RS
                if(input_div[0].equals("R"))
                {
                    // add garbage for kmeans
                    KmeansP_xlist.add(Double.parseDouble(input_div[1]));
                    KmeansP_ylist.add(Double.parseDouble(input_div[2]));
                    context.write(new Text("R"), new Text( input_div[1] + " " + input_div[2] ) );
                }
                else if( input_div[0].equals("C") )
                {
                    context.write(null, val);
                }
                else if(input_div[0].equals("DS_Cluster"))
                {
                    int index = Integer.parseInt(input_div[1]);  
                    //add centroid parameters into cluster info(initialization)
                    centroid.add(val.toString());
                    output_N[index] = Double.parseDouble(input_div[2]);
                    output_sum[0][index] = Double.parseDouble(input_div[3]);
                    output_sum[1][index] = Double.parseDouble(input_div[4]);
                    output_sumsq[0][index] = Double.parseDouble(input_div[5]);
                    output_sumsq[1][index] = Double.parseDouble(input_div[6]);
                }
                else if(input_div[0].equals("COUNT"))
                {
                    index_count = Integer.parseInt(input_div[1]);
                }
                else{
                    context.write(null, val);
                }
            }
        }
    }

    public double EuclideanDistance(double x1, double y1, double x2, double y2)
    {
            double EucDis;
            EucDis = Math.sqrt( Math.pow((x1-x2), 2) + Math.pow((y1-y2), 2) );
            return EucDis;
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        boolean[] cluster_flag = new boolean[x_list.size()]; 
		Arrays.fill(cluster_flag, true);
        
        double[][] cen = new double[2][NUM];
        for(int i = 0; i < centroid.size(); i++){
            //calculate the centoid coordinate
            cen[0][i] = output_sum[0][i]/output_N[i];
            cen[1][i] = output_sum[1][i]/output_N[i];
        }
        
        //add new points into old clusters
        for(int i = 0; i < x_list.size(); i++)
        {
            boolean check = false;
            double temp_maha_dist = 0.0;
            int cluster_num = -1;
            double[] coord = new double[2];
            coord[0] = x_list.get(i);
            coord[1] = y_list.get(i);
            for(int j = 0; j < centroid.size(); j++)
            {
                double mahalanobis_dist = 0.0;
                for(int k = 0; k < 2; k++) // get mahalanobis distance of all dimensions k 
                {
                    double sigma = output_sumsq[k][j]/output_N[j] - Math.pow(output_sum[k][j]/output_N[j], 2);
                    sigma = Math.abs(sigma);
                    sigma = Math.sqrt(sigma);
                    if(output_N[j]==1)
                    {
                        sigma = 1.0;
                    }
                    mahalanobis_dist += Math.pow( (coord[k]-cen[k][j])/sigma, 2 );
                }
                mahalanobis_dist = Math.sqrt(mahalanobis_dist);

                if(mahalanobis_dist < thresold && check == false)
                {
                    check = true;
                    temp_maha_dist = mahalanobis_dist;
                    cluster_num = j;
                }
                else if(mahalanobis_dist < thresold && check == true)
                {
                    if(temp_maha_dist > mahalanobis_dist)
                    {
                        temp_maha_dist = mahalanobis_dist;
                        cluster_num = j;
                    }
                }
            }
            if(check == true)
            {
                output_N[cluster_num] += 1;
                output_sum[0][cluster_num] += coord[0];
                output_sum[1][cluster_num] += coord[1];
                output_sumsq[0][cluster_num] += Math.pow( coord[0], 2 );
                output_sumsq[1][cluster_num] += Math.pow( coord[1], 2 );
                cluster_flag[i] = false;
                String out = String.valueOf(cluster_num) + " " + String.valueOf(coord[0]) + " " + String.valueOf(coord[1]);
                context.write( new Text("C"), new Text(out) );
            }
        }
        
        for(int i = 0; i < x_list.size(); i++)
        {
            if(cluster_flag[i] == true) // points which dont belong to any cluster
            {
                double[] coord = new double[2];
                coord[0] = x_list.get(i);
                coord[1] = y_list.get(i);
                RS_xlist.add(coord[0]);
                RS_ylist.add(coord[1]);
            }
        }
        for(int i = 0; i < RS_xlist.size(); i++)
        {
            String output_RS_coord = String.valueOf(RS_xlist.get(i)) + " " + String.valueOf(RS_ylist.get(i));
            // add garbage for kmeans
            KmeansP_xlist.add(RS_xlist.get(i));
            KmeansP_ylist.add(RS_ylist.get(i));
            context.write( new Text("R"), new Text(output_RS_coord) );
        }
        for(int cluster_num = 0; cluster_num < NUM; cluster_num++)
        {
            //String key = String.valueOf(cluster_num);
            String cluster_info = String.valueOf(cluster_num) + " " +
                                String.valueOf(output_N[cluster_num]) + " " + 
                                String.valueOf(output_sum[0][cluster_num]) + " " + String.valueOf(output_sum[1][cluster_num]) + " " +
                                String.valueOf(output_sumsq[0][cluster_num]) + " " + String.valueOf(output_sumsq[1][cluster_num]); 
            context.write( new Text("DS_Cluster"), new Text(cluster_info) );
        }

        ArrayList<Double> cen_xlist = new ArrayList<Double>();
        ArrayList<Double> cen_ylist = new ArrayList<Double>();
        //get Kmeans initial centroids
        if(KmeansP_xlist.size() != 0){
            cen_xlist.add(KmeansP_xlist.get(0));
            cen_ylist.add(KmeansP_ylist.get(0));
            KmeansP_xlist.remove(0);
            KmeansP_ylist.remove(0);
        }
        

        double temp_dist = 0.0;
        int flag = -1;
        if(KmeansP_xlist.size() != 0){
            for(int i = 0; i < KmeansP_xlist.size(); i++)
            {
                double eucDist = EuclideanDistance(cen_xlist.get(0), cen_ylist.get(0), KmeansP_xlist.get(i), KmeansP_ylist.get(i));
                if(temp_dist < eucDist){
                    flag = i;
                    temp_dist = eucDist;
                }
            }
            if(flag != -1){
                cen_xlist.add(KmeansP_xlist.get(flag));
                cen_ylist.add(KmeansP_ylist.get(flag));
                KmeansP_xlist.remove(flag);
                KmeansP_ylist.remove(flag);
            }
        }
       
       
        
        flag = -1;
        temp_dist = -1;
        if(KmeansP_xlist.size() != 0){
            for(int i = 0; i < KmeansP_xlist.size(); i++)
            {
                double dist = 0.0;
                for(int j = 0; j < cen_xlist.size(); j++)
                {
                    dist += EuclideanDistance(cen_xlist.get(j), cen_ylist.get(j), KmeansP_xlist.get(i), KmeansP_ylist.get(i));
                }
                if(dist > temp_dist){
                    flag = i;
                    temp_dist = dist;
                }
            }
            if(flag != -1){
                cen_xlist.add(KmeansP_xlist.get(flag));
                cen_ylist.add(KmeansP_ylist.get(flag));
                KmeansP_xlist.remove(flag);
                KmeansP_ylist.remove(flag);
        }
        }
        

        // k means centroids
        for(int i=0; i<cen_xlist.size(); i++)
        {
                context.write(null, new Text("KmeansCen " + String.valueOf(index_count) + " " + String.valueOf(cen_xlist.get(i)) + " " + String.valueOf(cen_ylist.get(i)) ) );
                index_count++;
        }
        context.write(new Text("COUNT"), new Text(String.valueOf(index_count)));
    } 
  }

  // K means
  public static final int d = 2;
  public static final int centerNum = NUM;
  public static class KmeansMapper extends Mapper<Object, Text, Text, Text>
  {

		private Text word = new Text();		
		public Text testK = new Text();
        ArrayList<Double> KMCen_xlist = new ArrayList<Double>();
        ArrayList<Double> KMCen_ylist = new ArrayList<Double>();

        ArrayList<Double> RS_xlist = new ArrayList<Double>();
        ArrayList<Double> RS_ylist = new ArrayList<Double>();

        ArrayList<Double> cen_xlist = new ArrayList<Double>();
        ArrayList<Double> cen_ylist = new ArrayList<Double>();
        public double EuclideanDistance(double x1, double y1, double x2, double y2){
            double EucDis;
            EucDis = Math.sqrt( Math.pow((x1-x2), 2) + Math.pow((y1-y2), 2) );
            return EucDis;
        }

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
			String line = value.toString();
			String tokens[] = line.split("\\s+"); 
            if(tokens[0].equals("KmeansCen"))
            {
                context.write( new Text(tokens[0]), new Text( tokens[1] + " " + tokens[2] + " " + tokens[3]) );
                /*
                KMCen_xlist.add(Double.parseDouble(tokens[1]));
                KMCen_ylist.add(Double.parseDouble(tokens[2]));
                */
            }
            else if(tokens[0].equals("R"))
            {
                context.write( new Text("R"), new Text(tokens[1] + " " + tokens[2]) );
                /*
                RS_xlist.add(Double.parseDouble(tokens[1]));
                RS_ylist.add(Double.parseDouble(tokens[2]));
                */
            }
            else
            {
                context.write(new Text("old"), new Text(value.toString()));
            }
		}
        /*
        public void cleanup(Context context) throws IOException, InterruptedException{
            int centroid = -1;
			double cost = Double.MAX_VALUE;
            double kmeans_threshold = 100.0;
            for(int i = 0; i < RS_xlist.size(); i++){
                for(int j = 0; j < KMCen_xlist.size(); j++)
                {
                    double sum = 0.0;
                    sum += Math.pow( KMCen_xlist.get(j) - RS_xlist.get(i), 2 );
                    sum += Math.pow( KMCen_ylist.get(j) - RS_ylist.get(i), 2 );
                    sum = Math.sqrt(sum);
                    if(sum < cost)
                    {
                        centroid = j;
                        cost = sum;
                    }
                }
                if(cost < kmeans_threshold)
                {

                }
                String out = String.valueOf(centroid) + " "+ String.valueOf(RS_xlist.get(i)) + " " + String.valueOf(RS_ylist.get(i));
		    	context.write( new Text("KmeansCen"), new Text(out));
            }
        }
        */
	}

	public static class KmeansReducer extends Reducer<Text,Text,Text,Text>
    {
		private Text result = new Text();
        ArrayList<Double> KMCen_xlist = new ArrayList<Double>();
        ArrayList<Double> KMCen_ylist = new ArrayList<Double>();
        ArrayList<Integer> KMCen_index = new ArrayList<Integer>();

        ArrayList<Double> RS_xlist = new ArrayList<Double>();
        ArrayList<Double> RS_ylist = new ArrayList<Double>();

        ArrayList<Double> cen_xlist = new ArrayList<Double>();
        ArrayList<Double> cen_ylist = new ArrayList<Double>();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
			if(key.toString().equals("old")){
                for(Text val: values){
                    context.write(null, val);	
                }
			}
            else if(key.toString().equals("KmeansCen"))
            {
                for(Text val: values)
                {
                    String line = val.toString();
                    String[] tokens = line.split("\\s+");
                    KMCen_index.add(Integer.parseInt(tokens[0]));
                    KMCen_xlist.add(Double.parseDouble(tokens[1]));
                    KMCen_ylist.add(Double.parseDouble(tokens[2]));
                }
            }
			else
            {
                for(Text val : values)
                {
                    String line = val.toString();
                    String[] tokens = line.split("\\s+");
                    
                    RS_xlist.add(Double.parseDouble(tokens[0]));
                    RS_ylist.add(Double.parseDouble(tokens[1]));

                }
                /*
				double newCentroid[] = new double[d];
				int pointNum = 0;
				for (Text val : values)
                {
					String line = val.toString();
					String tokens[] = line.split("\\s+"); 
					for(int i=0; i<d; i++){
						newCentroid[i] += Double.parseDouble(tokens[i]);
					}
					pointNum += 1;					
				}
				String centroidStr = "";
				for(int i=0; i<d; i++){
					newCentroid[i] = newCentroid[i]/(double)pointNum;
					centroidStr += String.valueOf(newCentroid[i]);
					centroidStr += " ";
				}
				context.write(new Text("KmeansCen"), new Text(centroidStr));
                */
			}
		}
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            for(int i = 0; i < RS_xlist.size(); i++)
            {
                double cost = Double.MAX_VALUE;
                double kmeans_threshold = 100.0;
                int cluster_num = -1;
                for(int j = 0; j < KMCen_xlist.size(); j++)
                {
                    double sum = 0.0;
                    sum += Math.pow( KMCen_xlist.get(j) - RS_xlist.get(i), 2 );
                    sum += Math.pow( KMCen_ylist.get(j) - RS_ylist.get(i), 2 );
                    sum = Math.sqrt(sum);
                    if(sum < cost && sum < kmeans_threshold)
                    {
                        cluster_num = j;
                        cost = sum;
                    }
                }
                if(cluster_num == -1)
                {
                    context.write(new Text("R"), new Text(String.valueOf(RS_xlist.get(i)) + " " + String.valueOf(RS_ylist.get(i)) ));
                }
                else
                {
                    context.write(new Text("CS"), new Text(String.valueOf(KMCen_index.get(cluster_num)) + " " + String.valueOf(RS_xlist.get(i)) + " " + String.valueOf(RS_ylist.get(i)) ));
                }
            }
            for(int i = 0; i < KMCen_index.size(); i++){
                context.write(new Text("KmeansCen"), new Text(String.valueOf(KMCen_index.get(i)) + " " + String.valueOf(KMCen_xlist.get(i)) + " " + String.valueOf(KMCen_ylist.get(i)) ));
            }
        }
        
	}

  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    int i, iter_time = 20;
    if (otherArgs.length != 2) {
      System.exit(2);
    }

    Job job = Job.getInstance();
  	
  	job.setJarByClass(BFR.class);
    job.setMapperClass(Initial_DS_Mapper.class);
    job.setReducerClass(Initial_DS_Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //FileInputFormat.addInputPath(job, new Path("/user/root/data/testcase.txt")) ;
   // FileInputFormat.addInputPath(job, new Path("/user/root/data/testcase1.txt")) ;
    //FileInputFormat.addInputPath(job, new Path("/user/root/data/testcase2.txt")) ;
    FileInputFormat.addInputPath(job, new Path( otherArgs[0] )) ;
	FileOutputFormat.setOutputPath(job, new Path("output/round_1"));
    job.waitForCompletion(true);

    for(i = 1; i <= 4; i++){
        job.cleanupProgress();
        job = Job.getInstance();
        job.addCacheFile(new URI("BFR/out" + Integer.toString(i) + ".txt#centroid" +  Integer.toString(i+1)));
        job.setJarByClass(BFR.class);
        job.setMapperClass(DS_Mapper.class);
        job.setReducerClass(DS_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(new URI( "output/round_" +  Integer.toString(i) +"/part-r-00000")));
        FileOutputFormat.setOutputPath(job, new Path( "output/KMeans_" + Integer.toString(i) + "0"));
        job.waitForCompletion(true);

        // k means        
        // job.addCacheFile(new URI("BFR/round_"+ Integer.toString(i+1) +  "#kmeans" ));
        int iter = 10;
        for(int j = 0; j < iter; j++){
            job.cleanupProgress();
            job = Job.getInstance();
            job.setJarByClass(BFR.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("output/KMeans_"+ Integer.toString(i) +Integer.toString(j) +"/part-r-00000")) ;
            //FileInputFormat.addInputPath(job, new Path( otherArgs[0] )) ;
            if(j == iter - 1)
                FileOutputFormat.setOutputPath(job, new Path("output/round_" +  Integer.toString(i+1)));
            else
                FileOutputFormat.setOutputPath(job, new Path("output/KMeans_"+ Integer.toString(i) + Integer.toString(j+1)));
            job.waitForCompletion(true);
        }
    }

    


    System.exit(0);
  }
}
