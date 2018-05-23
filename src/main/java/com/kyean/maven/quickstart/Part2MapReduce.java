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

public class Part2MapReduce {

	//------------------------------ Mapper and Reducer Job 1 -------------------------------------//

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

                    Text outputValue = new Text("r,"+ words[2]);
                    con.write(outputKey, outputValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //Reducer function for the mappers moviesReadMappper and reviewsReadMapper which reduces on the keys.
    public static class ReduceForWordCount extends Reducer<Text,Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values, Context con) throws IOException,
            InterruptedException {
            Long sum=0L;
            Double  movieRating=0.0;
            Text  movieName=null;
            for (Text value : values) {
                String line = value.toString();
                String[] words = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //Regular expression which only splits when a comma is not surrounded by ""

                if(words[0].contains("r")){

                    sum=sum+1;
                    movieRating=movieRating+ Double.valueOf(words[1]);

                }else{


                    movieName=new Text(words[1]);

                }
            }

            movieRating=movieRating/sum;
            
            //Only write movies down if they have more than 10 review and an average rating greater than 0
            if(sum>10 && movieRating>4) {

                con.write(movieName, new Text("num" + sum.toString()+ ",rating," + movieRating.toString() ));
            }
        }
    }

	//------------------------------ Mapper and Reducer Job 2 -------------------------------------//
    //Mapper on the file created for Job2 which creates the key value pairs with keys being the average and the value the rest.
    public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {


            try {
                if (value.toString().contains("movieId") /*Some condition satisfying it is header*/) {
                    return;
                }
                else {
                    String line = value.toString();
                    String[] words = line.split(",rating,");
                    Text outputValue = new Text(words[0]);


                    Text outputKey=new Text(words[1]);

                    con.write(outputKey,outputValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    //Reducer function which does not actually reduce anything, it simply used to sort the key value pairs from the mapper.
    public static class SortReducer extends Reducer<Text,Text, Text, Text> {
        public void reduce(Text word, Iterable<Text> values, Context con) throws IOException,
            InterruptedException {
            for (Text value : values) {
                String line = value.toString();
                String[] words = line.split("num");

                con.write(null, new Text(words[0]+ " " + word + " " + words[1]));
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
    	
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        //Set Paths
        //Set Paths
        Path input1 = new Path(args[0]); //Path for the movies.csv
        Path input2 = new Path(args[1]); //Path for the reviews.csv
        Path temporaryOutput = new Path(args[2] + "/Part2TemporaryOutput/"); //Path for temporary file
        Path finalOutput = new Path(args[2] + "/Part2FinalOuput/");//Path for final file
        
        //Path input1= new Path("/home/kevin/Documents/BigData/DataSet1_small/movies.csv");
        //Path input2 = new Path("/home/kevin/Documents/BigData/DataSet1_small/reviews.csv");
        //Path temporaryOutput = new Path("/home/kevin/Desktop/Part2TemporaryOutput");
        //Path finalOutput = new Path("/home/kevin/Desktop/Part2FinalOutput");

        //-------------------------Job 1 ----------------------/
        System.out.println("Job 1: START");
        Job job1 = new Job(config, "ReduceSideJoinRating");
        job1.setJarByClass(Part2MapReduce.class);
        //Test
        
        
        //Setup Map Reduce
        job1.setMapperClass(Part2MapReduce.moviesReadMapper.class);
        job1.setMapperClass(Part2MapReduce.reviewsReadMapper.class);

        //Setup Reducers
        job1.setReducerClass(Part2MapReduce.ReduceForWordCount.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1,TextInputFormat.class, Part2MapReduce.moviesReadMapper.class);
        MultipleInputs.addInputPath(job1, input2,TextInputFormat.class, Part2MapReduce.reviewsReadMapper.class);
        FileOutputFormat.setOutputPath(job1, temporaryOutput);
        //Job1 completion
        boolean sucess = job1.waitForCompletion(true);
        System.out.println("Job 1: END");
        if(sucess) {
        	//--------------------------- Job 2 -------------------------//
        	System.out.println("Job 2: START ");
            Job job2 = new Job(config, "Sorting");
            job2.setJarByClass(Part2MapReduce.class);
                        
            //Set MapReduce
            job2.setMapperClass(Part2MapReduce.SortMapper.class);
            
            //Set Reducer
            job2.setReducerClass(Part2MapReduce.SortReducer.class);

            //Set Paths
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job2, temporaryOutput,TextInputFormat.class, Part2MapReduce.SortMapper.class);
            FileOutputFormat.setOutputPath(job2, finalOutput);
        	job2.setNumReduceTasks(1);
        	
        	//Job 2 Task Completion
            job2.waitForCompletion(true);
            System.out.println("Job 2: END");
        }
        System.exit(0);
    }
}