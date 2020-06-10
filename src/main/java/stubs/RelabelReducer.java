package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelabelReducer extends Reducer<IntWritable, Text, Text, Text > {

    int idx = 1;


    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

        Text rowNum = new Text();

        for (Text value : values) {
            rowNum.set(String.valueOf(idx));
            context.write(rowNum, value);
            idx++;
        }

    }

}