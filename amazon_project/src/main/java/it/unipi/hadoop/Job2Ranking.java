package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Job2Ranking
{
    public static class RankingMapper extends Mapper<LongWritable, Text, CategoryScoreKey, Text>
    {
        private final CategoryScoreKey outputKey = new CategoryScoreKey();
        private final Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();

            if (line == null || line.length() == 0)
                return;

            //extract the fields from output of Job1
            String[] fields = line.split("\t");
            String productParent = fields[0];
            String[] data = fields[1].split("###");
            String productTitle = data[0];
            String category = data[1];
            int reviewCount = Integer.parseInt(data[2]);
            double avgRating = Double.parseDouble(data[3]);
            int verifiedCount = Integer.parseInt(data[4]);
            int helpfulVotes = Integer.parseInt(data[5]);
            int totalVotes = Integer.parseInt(data[6]);

            //compute the score
            double score = avgRating*Math.log(1+reviewCount)*(1+((double)helpfulVotes/(1+totalVotes)));

            outputKey.set(category, score);
            outputValue.set( productParent + "###" + productTitle + "###" + score);

            context.write(outputKey, outputValue);
        }
    }

    public static class CategoryPartitioner extends Partitioner<CategoryScoreKey, Text>
    {
        @Override
        public int getPartition(CategoryScoreKey key, Text value, int numPartitions)
        {
            return Math.abs(key.getCategory().hashCode()) % numPartitions;
        }
    }

    public static class RankingReducer extends Reducer<CategoryScoreKey, Text, Text, Text>
    {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        private String currentCategory = "";
        private int rank = 0;

        @Override
        public void reduce(CategoryScoreKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            if (!key.getCategory().equals(currentCategory)) {
                currentCategory = key.getCategory();
                rank = 1;
            }

            for (Text val : values) {
                String[] fields = val.toString().split("###");

                String productParent = fields[0];
                String productTitle = fields[1];
                String score = fields[2];

                outputKey.set(productParent);
                outputValue.set(productTitle + "###" + currentCategory + "###" + score + "###" + rank);

                context.write(outputKey, outputValue);

                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Job2Ranking <input> <output> <reducers>");
            System.exit(1);
        }

        System.out.println("args[0]: <input>=" + otherArgs[0]);
        System.out.println("args[1]: <output>=" + otherArgs[1]);

        Job job = Job.getInstance(conf, "Job2Ranking");
        job.setJarByClass(Job2Ranking.class);
        job.setMapperClass(RankingMapper.class);
        job.setPartitionerClass(CategoryPartitioner.class);
        job.setReducerClass(RankingReducer.class);
        
        int reducers = Integer.parseInt(otherArgs[2]);
        job.setNumReduceTasks(reducers);

        job.setMapOutputKeyClass(CategoryScoreKey.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);        
        job.setOutputFormatClass(TextOutputFormat.class);
     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}