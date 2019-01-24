package com.example.wordcount.reducer;

import com.example.wordcount.utils.WCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        for (IntWritable i : values) {
            counter += i.get();
        }
        context.write(key, new IntWritable(counter));
        String word = key.toString();
        if (!word.isEmpty()) {
            Character firstChar = word.charAt(0);
            context.getCounter("CustomCounters", "Total Words").increment(1);
            if (Character.isDigit(firstChar)) {
                context.getCounter("CustomCounters", WCounters.Words.DIGIT.name()).increment(1);
            } else {
                switch (Character.toLowerCase(firstChar)) {
                    case 'a':
                    case 'e':
                    case 'i':
                    case 'o':
                    case 'u': {
                        context.getCounter("CustomCounters", WCounters.Words.VOWEL.name()).increment(1);
                        break;
                    }
                    default: {
                        context.getCounter("CustomCounters", WCounters.Words.CONSONANT.name()).increment(1);
                    }
                }
            }
        }
    }
}
