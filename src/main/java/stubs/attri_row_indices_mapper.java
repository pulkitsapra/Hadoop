package stubs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;


public class attri_row_indices_mapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        Text text = new Text();
        String rowno=null;
        String line = value.toString();
        int ct=0;
        for (String word : line.split("\\s+")) {
            {
                if(ct==0){
                    rowno=word;
                }
                else{
                    text.set(word);
                    context.write(text,new Text(rowno));
                }
                ct++;

            }
        }
    }
}