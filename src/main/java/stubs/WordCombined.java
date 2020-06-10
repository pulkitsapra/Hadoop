package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;


public class WordCombined extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        JobControl jobControl = new JobControl("jobChain");

//**************************************************** JOB 1 {simple Word Count} Configuration **********************************
        Configuration conf1 = getConf();
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(WordCombined.class);
        job1.setJobName("Word Combinator");

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setCombinerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);

        jobControl.addJob(controlledJob1);
        /*
//**************************************************** JOB 2 Configuration{ (freq,word) in sorted order} **********************************
        Configuration conf2 = getConf();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCombined.class);
        job2.setJobName("Word Invert");

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/temp2"));

        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(SumReducer2.class);
        job2.setCombinerClass(SumReducer2.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(IntWritable.class);

        job2.setSortComparatorClass(IntComparator.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);
//**************************************************** JOB 3 (Relabel and sorting) Configuration **********************************

        Configuration conf3 = getConf();
        Job job3 = Job.getInstance(conf3);
        job3.setJarByClass(WordCombined.class);
        job3.setJobName("Relabel");

        FileInputFormat.setInputPaths(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/relabeled"));

        job3.setMapperClass(RelabelMapper.class);
        job3.setReducerClass(RelabelReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setNumReduceTasks(1);


        FileSystem fs = FileSystem.get(conf3);

        job3.addCacheFile(new Path(args[1] + "/temp2/part-r-00000").toUri());

        ControlledJob controlledJob3 = new ControlledJob(conf3);
        controlledJob3.setJob(job3);

        // make job3 dependent on job1
        controlledJob3.addDependingJob(controlledJob2);
        // add the job to the job control
        jobControl.addJob(controlledJob3);


//**************************************************** Job-4 Generating <attribute, array of rows>*******

        Configuration conf4 = getConf();
        Job job4 = Job.getInstance(conf4);
        job4.setJarByClass(WordCombined.class);
        job4.setJobName("Attri-row-indices");

        FileInputFormat.setInputPaths(job4, new Path(args[1] + "/relabeled/part-r-00000"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/attri-row-indices"));

        job4.setMapperClass(attri_row_indices_mapper.class);
        job4.setReducerClass(attri_row_indices_reducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(Text.class);

        ControlledJob controlledJob4 = new ControlledJob(conf4);
        controlledJob4.setJob(job4);



        controlledJob4.addDependingJob(controlledJob3);
        jobControl.addJob(controlledJob4);



//**************************************************** Final Job - Multiple Outputs *********************


        Configuration conf5 = getConf();
        Job job5 = Job.getInstance(conf5);
        job5.setJarByClass(WordCombined.class);
        job5.setJobName("final-out");

        FileInputFormat.setInputPaths(job5, new Path(args[1] + "/groupings/groups"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "/final"));

        job5.setMapperClass(final_mapper.class);
        job5.setReducerClass(final_reducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setMapOutputKeyClass(Text.class);

        ControlledJob controlledJob5 = new ControlledJob(conf5);
        controlledJob4.setJob(job5);

        FileSystem f = FileSystem.get(conf5);

        job5.addCacheFile(new Path(args[1] + "/attri-row-indices/part-r-00000").toUri());


        controlledJob5.addDependingJob(controlledJob4);
        jobControl.addJob(controlledJob5);

*/
//**************************************************** Run Job Control **********************************
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//            System.out.println("The file exists - " + fs.exists(new Path(args[1] + "/temp2/part-r-00000")));
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.exit(0);
        return (job1.waitForCompletion(true) ? 0 : 1);
    }



    public static void main(String[] args) throws Exception {
        // int exitCode = ToolRunner.run(new WordCombined(), args);
        // System.exit(0);

        System.out.println("Heya\n");
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1);
        job1.setJarByClass(WordCombined.class);
        job1.setJobName("Word Combinator");

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setCombinerClass(SumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        if(!job1.waitForCompletion(true)){
            System.exit(2);
        }


        /*

        //**************************************************** JOB 2 Configuration{ (freq,word) in sorted order} **********************************
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(WordCombined.class);
        job2.setJobName("Word Invert");

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/temp2"));

        job2.setMapperClass(WordMapper2.class);
        job2.setReducerClass(SumReducer2.class);
        job2.setCombinerClass(SumReducer2.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(IntWritable.class);

        job2.setSortComparatorClass(IntComparator.class);

        if(!job2.waitForCompletion(true)){
            System.exit(2);
        }

//**************************************************** JOB 3 (Relabel and sorting) Configuration **********************************

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);
        job3.setJarByClass(WordCombined.class);
        job3.setJobName("Relabel");

        FileInputFormat.setInputPaths(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/relabeled"));

        job3.setMapperClass(RelabelMapper.class);
        job3.setReducerClass(RelabelReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setNumReduceTasks(1);


        FileSystem fs = FileSystem.get(conf3);

        job3.addCacheFile(new Path(args[1] + "/temp2/part-r-00000").toUri());


        if(!job3.waitForCompletion(true)){
            System.exit(2);
        }


//**************************************************** Job-4 Generating <attribute, array of rows>*******

        Configuration conf4 =new Configuration();
        Job job4 = Job.getInstance(conf4);
        job4.setJarByClass(WordCombined.class);
        job4.setJobName("Attri-row-indices");

        FileInputFormat.setInputPaths(job4, new Path(args[1] + "/relabeled/part-r-00000"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/attri-row-indices"));

        job4.setMapperClass(attri_row_indices_mapper.class);
        job4.setReducerClass(attri_row_indices_reducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(Text.class);


        if(!job4.waitForCompletion(true)){
            System.exit(2);
        }

        relfreq(Integer.parseInt(args[2]));


//**************************************************** Final Job - Multiple Outputs *********************


        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5);
        job5.setJarByClass(WordCombined.class);
        job5.setJobName("final-out");

        FileInputFormat.setInputPaths(job5, new Path(args[1] + "/groupings/groups"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "/final"));

        job5.setMapperClass(final_mapper.class);
        job5.setReducerClass(final_reducer.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setMapOutputKeyClass(Text.class);

        FileSystem f = FileSystem.get(conf5);

        job5.addCacheFile(new Path(args[1] + "/attri-row-indices/part-r-00000").toUri());

        if(!job5.waitForCompletion(true)){
            System.exit(2);
        }


    }





    public static void relfreq(Integer threshold) throws Exception {
        HashMap<String, Integer> relabelfreqMap;
        relabelfreqMap = new HashMap<String, Integer>();

        String uri = "/output/temp2/part-r-00000";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path("/output/temp2/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
        String line = null;
        int idx = 1;
        while ((line = reader.readLine()) != null) {
            String[] vals = line.split("\\t");
            String val = vals[0];
            relabelfreqMap.put(Integer.toString(idx), Integer.parseInt(val));
            idx++;

        }

        ArrayList<Integer> listOfKeys = new ArrayList<Integer>();

        int i =1;
        while(i<(idx)){
            listOfKeys.add(i);
            i++;
        }

        HashMap<String, String> Grp;
        Grp = new HashMap<String, String>();

        ListIterator<Integer> itr = listOfKeys.listIterator(listOfKeys.size());

        int countthresh=0;
        int groups =1;
        String grpmembers="";

        while(itr.hasPrevious()) {
            String key =itr.previous().toString() ;
            if(relabelfreqMap.get(key) > threshold){

                Grp.put(Integer.toString(groups),grpmembers);
                grpmembers="";
                grpmembers=key+" ";
                groups++;
                while(itr.hasPrevious()){
                    String k=itr.previous().toString();
                    grpmembers = grpmembers + k + " ";
                }
                Grp.put(Integer.toString(groups),grpmembers);

            }
            else if(countthresh+relabelfreqMap.get(key) <= threshold){
                countthresh+=relabelfreqMap.get(key);
                grpmembers = grpmembers + key + " ";
            }

            else{
                Grp.put(Integer.toString(groups),grpmembers);
                countthresh=relabelfreqMap.get(key);

                grpmembers=""+key+" ";
                groups++;
            }
        }
        Grp.put(Integer.toString(groups),grpmembers);

        FileSystem fs_1 = FileSystem.get(new Configuration());
        OutputStream os = fs_1.create(new Path("/output/groupings/groups"));
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        ListIterator<Integer> itr1 = listOfKeys.listIterator(0);
        int grp=0;
        while(grp < groups){
            String key = itr1.next().toString();
            System.out.println(key + " : " + Grp.get(key) + "\n");
            br.write(key + " : " + Grp.get(key) + "\n" );
            grp++;

        }
        br.close();
        os.close();
        fs_1.close();

        */

    }
}