package org.wuruihe.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
      //  args[0]= new String("file/wordcount/hello.txt");
        //args[0]=new String ("wordcount.txt");
        //args[1]=new String("G:\\helloout.txt");
        args=new String[]{"wordcount.txt","g:/wordout"};
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        job.setJarByClass(WordcountDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);

    }
}
