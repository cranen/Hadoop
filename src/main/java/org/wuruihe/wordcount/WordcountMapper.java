package org.wuruihe.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text k=new Text();
    IntWritable v=new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String s = value.toString();
        //切割
        String[] split = s.split(" ");
        //输出
        for (String word : split) {
          //  k.set(word);
            k.set(word);
            context.write(k,v);
        }

    }
}
