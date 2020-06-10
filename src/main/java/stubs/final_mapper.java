package stubs;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class final_mapper extends Mapper<Object, Text, Text, Text> {
    private HashMap<String, String> mp;


    @Override
    public void setup(Context context) {
        try {

            mp= new HashMap<String, String>();
            URI[] grps = context.getCacheFiles();
            if (grps != null && grps.length > 0) {
                for (URI grp : grps) {
                    Path file = new Path(grp);
                    BufferedReader reader = new BufferedReader(new FileReader(file.getName()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        String[] vals = line.split( "\\t");
                        String attr = vals[0];
                        mp.put(attr, vals[1]);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        HashSet<String> hash_set = new HashSet<String>();
        HashSet<String> hash_set_new = new HashSet<String>();
        String line = value.toString();
        String [] grp = line.split(" : ");
        String [] grpattr = grp[1].split(" ");
        String row="";
        for (String stritr : grpattr){
            String [] lineos = mp.get(stritr).split(" ");
            for(String s:lineos){
                System.out.println(s);
                hash_set.add(s);
//                hash_set= Sets.union(hash_set,hash_set_new);
            }

            Iterator<String> i = hash_set.iterator();
            while(i.hasNext()){
                row+=i.next() + " ";
            }

        }

        context.write(new Text(grp[0]),new Text(row));
    }

}