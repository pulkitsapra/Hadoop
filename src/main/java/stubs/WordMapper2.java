package stubs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper2 extends Mapper< Object, Text, IntWritable, Text> {

    IntWritable frequency = new IntWritable();



    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] xyz = value.toString().split("\\s+");
        frequency.set(Integer.parseInt(xyz[1]));
        context.write(frequency, new Text(xyz[0]));

        }
    }