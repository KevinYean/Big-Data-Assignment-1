package com.kyean.maven.quickstart;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part1MapReduce {


//----------------------------------- Mapper and Reducers for Job 1 -----------------------------------------------//	
	
	//Mapper function for movies.csv files which create key value pairs with the keys being the movie ID and the value being the movie name.
    public static class moviesReadMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            try {
                if (value.toString().contains("movieId") /*Some condition satisfying it is header*/) {
                    return;
                }
                else {
                    String line = value.toString();
                    String[] words = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //Regular expression which only splits when a comma is not surrounded by ""
                    
                    Text outputValue = new Text("m,"+ words[1]);
                    Text outputKey=new Text(words[0]);

                        con.write(outputKey, outputValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Mapper function for reviews.csv which creates key value pairs with they keys being the movieID and the value being the movie name.
    public static class reviewsReadMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            try {
                if (value.toString().contains("movieId") /*Some condition satisfying it is header*/) {
                    return;
                }
                else {
                    String line = value.toString();
                    String[] words = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //Regular expression which only splits when a comma is not surrounded by ""

                    Text outputKey=new Text(words[1]);

                    Text outputValue = new Text("r");
                    con.write(outputKey, outputValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Reducer function for the mappers moviesReadMappper and reviewsReadMapper which reduces on the keys.
    public static class ReduceForWordCount extends Reducer<Text,Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Long sum=0L;
            Text  movieName=null;
            int movieCount;
            for (Text value : values) {
                String line = value.toString();
                String[] words = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");;

                if(words[0].contains("r")){
                    sum=sum+1;

                }else{
                  movieName=new Text(words[1]);

                }

            }
            if(sum!=0) { //Only write movies down if they have more than 1 review.
	            String sumString = sum.toString();
	            while(sumString.toCharArray().length < 5) {
	            	sumString = '0' + sumString;
	            }
	            context.write(new Text(sumString.toString()),movieName );
            }
        }
    }
    
 //---------------------------------Mapper and Reducer for Job 2 ----------------------------------------------------//
    
    //Mapper on the file created for Job2 which creates the key value pairs with keys being the number occurences and the value the movie ID.
    public static class SortingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        	
        	 String line = value.toString();
             String[] words = line.split("	");
             Text outputValue = new Text(words[1]);
             Text outputKey=new Text(words[0]);
        	
        	con.write(outputKey, outputValue);
        }
    }
    
    //Reducer function which does not actually reduce anything, it simply used to sort the key value pairs from the mapper.
    public static class SortingReducer extends Reducer<Text,Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
        	
        	Text sum = null;
        	
        	 for (Text value : values) {
        		  String line = value.toString();
        		  sum = new Text(line);
        		  context.write(word, new Text (sum));
        	 }
             
			
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
        
        //Set Paths
        Path input1 = new Path(args[0]); //Path for the movies.csv
        Path input2 = new Path(args[1]); //Path for the reviews.csv
        Path temporaryOutput = new Path(args[2] + "/Part1TemporaryOutput/"); //Path for temporary file
        Path finalOutput = new Path(args[2] + "/Part1FinalOuput/");//Path for final file
        
        //Hard Coded Paths
        //Path input1= new Path("/home/kevin/Documents/BigData/DataSet1_small/movies.csv");
        //Path input2 = new Path("/home/kevin/Documents/BigData/DataSet1_small/reviews.csv");
        //Path temporaryOutput = new Path("/home/kevin/Desktop/Part1TemporaryOutput/");
        //Path finalOutput = new Path("/home/kevin/Desktop/Part1FinalOuput/");        

        //------- Job 1 --------------------------------------------------//
    	System.out.println("Job 1: START");
        Job job1 = new Job(config, "ReduceSideJoinPopCount");
        job1.setJarByClass(Part1MapReduce.class);
        
    	//Setup MapReduce
        job1.setMapperClass(Part1MapReduce.moviesReadMapper.class);
        job1.setMapperClass(Part1MapReduce.reviewsReadMapper.class);

    	//Setup Reducer
        job1.setReducerClass(Part1MapReduce.ReduceForWordCount.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1,TextInputFormat.class, Part1MapReduce.moviesReadMapper.class);
        MultipleInputs.addInputPath(job1, input2,TextInputFormat.class, Part1MapReduce.reviewsReadMapper.class);
        FileOutputFormat.setOutputPath(job1, temporaryOutput);
        //Job 1 Task Completion
        job1.setNumReduceTasks(1);
        boolean sucess = job1.waitForCompletion(true);
        System.out.println("Job 1: END.");
        
        //--------------------------- Job 2 -----------------------------------//
        if(sucess) {
        	System.out.println("Job 2: START");
        	Job job2 = new Job(config, "Sorting");
        	
        	//Setup MapReduce
            job2.setJarByClass(Part1MapReduce.class);
        	job2.setMapperClass(Part1MapReduce.SortingMapper.class);
        	
        	//Setup Reducer
            job2.setReducerClass(Part1MapReduce.SortingReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job2, temporaryOutput,TextInputFormat.class, Part1MapReduce.SortingMapper.class);
            FileOutputFormat.setOutputPath(job2, finalOutput);
        	job2.setNumReduceTasks(1);
        	
        	//Job 2 Task Completion
            job2.waitForCompletion(true);
            System.out.println("Job 2: END.");
            
        }
        
        
        System.exit(0);
    }
}