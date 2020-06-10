package stubs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class final_reducer extends Reducer<Text, Text, Text, Text > {

    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        mos = new MultipleOutputs(context);

    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        for (Text value : values) {
            mos.write(key, value, key.toString());

        }

    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        mos.close();
    }

}