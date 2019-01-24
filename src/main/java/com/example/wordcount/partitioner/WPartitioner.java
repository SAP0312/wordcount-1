package com.example.wordcount.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        String word = text.toString();
        if (!word.isEmpty()) {
            Character firstChar = Character.toLowerCase(word.charAt(0));
            if (Character.isDigit(firstChar)) {
                return 26 % numPartitions;
            } else {
                return (firstChar - 'a') % numPartitions;
            }
        }
        return 0;
    }
}
