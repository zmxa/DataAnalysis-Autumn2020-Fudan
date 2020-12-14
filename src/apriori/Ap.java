package apriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.test.MapredTestDriver;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Ap extends Configured implements Tool {
    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	String hdfsInputDir;                        
        int maxPasses;            
        int minSup;
        int i;
        
        //These helps debug.
        if (args.length == 3) {
            hdfsInputDir = args[0];                        
            maxPasses = Integer.parseInt(args[1]);            
            minSup = Integer.parseInt(args[2]);
            i=1;
        }
        else {
        	hdfsInputDir = args[0];                        
            maxPasses = Integer.parseInt(args[1]);            
            minSup = Integer.parseInt(args[2]);
            i=Integer.parseInt(args[3]);
        }
                
        //The real circle.
        for (; i <= maxPasses; i++) {
            boolean isPassKJobDone = runPassKJob(hdfsInputDir, "a/output", i, minSup);
            if (!isPassKJobDone) {
                System.err.println("Fail.");
                return -1;
            }
        }
        return 1;
    }

    static boolean runPassKJob(String inputDir, String outputDirPrefix, int passNum, int minSup)
            throws IOException, InterruptedException, ClassNotFoundException {
        boolean isJobSuccess;
        Configuration conf = new Configuration();
        conf.setInt("passNum", passNum);
        conf.setInt("minSup", minSup);
        
        //Useless struggle.
        conf.set("mapreduce.map.java.opts", "-Xmx7192m");
        conf.set("mapreduce.map.memory.mb", "7192");
        conf.set("mapred.task.timeout", "0");
        
        
        System.out.println("Starting Phase" + passNum + "Job");       
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "job" + passNum);

        job.setJarByClass(Ap.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDirPrefix + passNum));

        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        if (passNum == 1) {
            job.setMapperClass(Map1.class);
        }
        //An optimization for map phase 2, instead of 
        //building up a tremendous useless trie structure and 
        //suffering a traversal on the whole "output1".
        //avoid the stupid negative optimization used by the following stage. 
        else if(passNum == 2){
            job.setMapperClass(Map2.class);
        }
        else {
        	job.setMapperClass(MapK.class);
        }
        
        job.setReducerClass(Reduce.class);

        isJobSuccess = (job.waitForCompletion(true) ? true : false);
        System.out.println("Finished Phase" + passNum + "Job");

        return isJobSuccess;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Ap(), args);
        System.exit(exitCode);
        
    }
}
