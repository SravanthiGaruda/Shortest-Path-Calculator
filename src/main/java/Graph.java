

import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                
    public int distance;               
    public Vector<Integer> following;  

    Tagged () { tag = false; distance = 0; following = new Vector<Integer>(); }
    Tagged ( int d ) { tag = false; distance = d; following = new Vector<Integer>();}
    Tagged ( int d, Vector<Integer> f ) { tag = true; distance = d; following = f; }

    public void write ( DataOutput out ) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (following != null) {
            out.writeInt(following.size());
            for (int i = 0; i < following.size(); i++) {
                out.writeInt(following.get(i));
            }
        } else {
            out.writeInt(0); 
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());
        }
    }
    
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    /* ... */
    public static class FirstMapper extends Mapper<Object,Text,IntWritable,IntWritable> 
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException 
        {
            String values[]=value.toString().split(",");
            int id = Integer.parseInt(values[0]);
            int follower = Integer.parseInt(values[1]);

            context.write(new IntWritable(follower),new IntWritable(id));
        }
    }  

    public static class FirstReducer extends Reducer<IntWritable,IntWritable,IntWritable,Tagged> 
    {
        // static Vector<Integer> following = new Vector<>();
        @Override
        public void reduce (IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            Vector<Integer> following = new Vector<>();
            for(IntWritable id :values){
                following.add(id.get());
            }
            if(key.get() == start_id || key.get() == 1){
                context.write(key, new Tagged(0, following));
            }else{
                context.write(key, new Tagged(Integer.MAX_VALUE, following));
            }
        }
    }

    public static class SecondMapper extends Mapper<IntWritable,Tagged,IntWritable,Tagged> 
    {
        @Override
        public void map(IntWritable key, Tagged value, Context context) throws IOException,InterruptedException 
        {
            context.write(key, value);
            if(value.distance < Integer.MAX_VALUE){
                for(Integer val:value.following){
                    context.write(new IntWritable(val), new Tagged(value.distance + 1));
                }
            }
        }
    } 

    public static class SecondReducer extends Reducer<IntWritable,Tagged,IntWritable,Tagged> 
    {
        static Vector<Integer> following = new Vector<>();
        @Override
        public void reduce (IntWritable key, Iterable<Tagged> values, Context context) throws IOException, InterruptedException 
        {
            int minDistance = Integer.MAX_VALUE;
            Vector<Integer> following = null;

            for (Tagged value : values) {
                if (value.distance < minDistance) {
                    minDistance = value.distance;
                }
                if (value.tag) {
                    following = value.following;
                }
            }

            context.write(key, new Tagged(minDistance, following));
        }
    }

    public static class ThirdMapper extends Mapper<IntWritable,Tagged,IntWritable,IntWritable> 
    {
        @Override
        public void map(IntWritable key, Tagged value, Context context) throws IOException,InterruptedException 
        {
            if ( value.distance < Integer.MAX_VALUE ){
                context.write(key, new IntWritable(value.distance));
            }
           
        }
    } 

    public static class ThirdReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job job = Job.getInstance();
        /* ... First Map-Reduce job to read the graph */
        /* ... */
        job.setJobName("JoinJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Tagged.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(FirstMapper.class); 
        job.setReducerClass(FirstReducer.class);  
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);

        for ( short i = 0; i < iterations; i++ ) {
            job = Job.getInstance();
            /* ... Second Map-Reduce job to calculate shortest distance */
            /* ... */
            job.setJarByClass(Graph.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Tagged.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Tagged.class);
            job.setMapperClass(SecondMapper.class);  
            job.setReducerClass(SecondReducer.class);  
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Last Map-Reduce job to output the distances */
        /* ... */
        job = Job.getInstance(); 
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(ThirdMapper.class); 
        job.setReducerClass(ThirdReducer.class); 
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+iterations));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}


