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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PopCount {


    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {


            try {
                if (key.get() == 0 && value.toString().contains("movieId") /*Some condition satisfying it is header*/) {


                    return;
                }
                else {
                    String line = value.toString();
                    String[] words = line.split(",");
                     IntWritable outputValue = new IntWritable(1);

                        Text outputKey = new Text(words[1]);
                        con.write(outputKey, outputValue);

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        System.out.println(files.length);




        // setup mapreduce job
        Job job = new Job(config, "wordcount");
        job.setJarByClass(PopCount.class);

        // setup mapper
        job.setMapperClass(PopCount.MapForWordCount.class);
        // setup reducer
        job.setReducerClass(PopCount.ReduceForWordCount.class);

        // set input/output path
        Path input = new Path("/home/m-93/Desktop/files/reviews.csv");
        Path output = new Path("/home/m-93/Desktop/hadoopTextFiles/output/");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        // task completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}