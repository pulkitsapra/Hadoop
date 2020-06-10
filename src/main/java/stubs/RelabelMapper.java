package stubs;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class RelabelMapper extends Mapper<Object, Text, IntWritable, Text> {
    private HashMap<String, Integer> relabelMap;

    @Override
    public void setup(Context context) {
        try {
            relabelMap = new HashMap<String, Integer>();
            URI[] frequencies = context.getCacheFiles();
            if (frequencies != null && frequencies.length > 0) {
                for (URI frequency : frequencies) {
                    Path file = new Path(frequency);
                    BufferedReader reader = new BufferedReader(new FileReader(file.getName()));
                    String line = null;
                    int idx = 1;
                    while ((line = reader.readLine()) != null) {
                        String[] vals = line.split("\\t");
                        String val = vals[1];
                        relabelMap.put(val, idx);
                        idx++;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        Text row = new Text();
        ArrayList<Integer> temp = new ArrayList<>();
        int count = 0;
        while(tokenizer.hasMoreTokens()){
            count=count+1;
            String token = tokenizer.nextToken();
            temp.add(relabelMap.get(token));
        }
        Collections.sort(temp);
        String currow="";
        for(Integer val : temp){
            currow=currow+val.toString() + " ";
        }
        row.set(currow);
        context.write(new IntWritable(-1*count),row);


    }

}