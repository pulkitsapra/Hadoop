package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Text word = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            word.set(value);
            context.write(key, word);
        }
    }
}