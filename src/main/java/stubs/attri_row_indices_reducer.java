package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class attri_row_indices_reducer extends Reducer<Text, Text, Text, Text > {

    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        String row ="";
        for (Text value : values) {
            row+=value.toString() + " ";
        }
        context.write(key,new Text(row));
    }
}