package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Job3Anomaly
{
    public static class AnomalyMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();

            if (line == null || line.length() == 0)
                return;

            String[] fields = line.split("\t");
            String productParent = fields[0];
            String[] data = fields[1].split("###");

            if (data.length == 7){
                outputKey.set(productParent);
                outputValue.set("JOB1###" + fields[1]);
                context.write(outputKey, outputValue);
            } 
            else if (data.length == 4){
                outputKey.set(productParent);
                outputValue.set("JOB2###" + fields[1]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class AnomalyReducer extends Reducer<Text, Text, Text, Text>
    {
        private final Text outputValue = new Text();
        private int perfectProducts;
        private int helpfulAnomalies;
        private int verifiedAnomalies;
        private int rankAnomalies;
        private int minReviewsPerfect;
        private double minHelpfulRatio;
        private double minVerifiedRatio;

        @Override
        public void setup(Context context)
        {
            Configuration conf = context.getConfiguration();
            minReviewsPerfect = conf.getInt("min.reviews.perfect", 100);
            minHelpfulRatio = conf.getDouble("min.helpful.ratio", 0.2);
            minVerifiedRatio = conf.getDouble("min.verified.ratio", 0.5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String productTitle = "";
            String category = "";
            int reviewCount = 0;
            double avgRating = 0;
            int verifiedCount = 0;
            int helpfulVotes = 0;
            int totalVotes = 0;
            double score = 0;
            int rank = 0;

            for (Text val : values)
            {
                String[] data = val.toString().split("###");

                //job1
                if (data[0].equals("JOB1")){
                    productTitle = data[1];
                    category = data[2];
                    reviewCount = Integer.parseInt(data[3]);
                    avgRating = Double.parseDouble(data[4]);
                    verifiedCount = Integer.parseInt(data[5]);
                    helpfulVotes =  Integer.parseInt(data[6]);
                    totalVotes = Integer.parseInt(data[7]);   
                } else if(data[0].equals("JOB2")){ //job2
                    productTitle = data[1];
                    category = data[2];
                    score = Double.parseDouble(data[3]);
                    rank = Integer.parseInt(data[4]);
                }
            }

            double helpfulRatio = (double) helpfulVotes/(1+totalVotes);
            double verifiedRatio = (double) verifiedCount/reviewCount;

            if (score > 20 && helpfulRatio < minHelpfulRatio){
                helpfulAnomalies++;
                outputValue.set("HELPFUL_RATIO_ANOMALY" + "###" + productTitle + "###" + category + "###" + score);
                context.write(key, outputValue);
            }

            if (verifiedRatio < minVerifiedRatio && reviewCount > 100){
                verifiedAnomalies++;
                outputValue.set("VERIFIED_RATIO_ANOMALY" + "###" + productTitle + "###" + category + "###" + score);
                context.write(key, outputValue);
            }

            if (avgRating > 4.9 && reviewCount > minReviewsPerfect){
                perfectProducts++;
                outputValue.set("SUSPICIOUS_PERFECT_PRODUCT" + "###" + productTitle + "###" + category + "###" + score);
                context.write(key, outputValue);
            }

            if (rank <= 10 && avgRating < 3.5){
                rankAnomalies++;
                outputValue.set("RANK_INCONSISTENCY" + "###" + productTitle + "###" + category + "###" + score);
                context.write(key, outputValue);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            context.write(new Text("[REDUCER_SUMMARY]"), new Text("PerfectProducts=" + perfectProducts + " HelpfulRatioAnomalies=" + helpfulAnomalies + " VerifiedRatioAnomalies=" + verifiedAnomalies + " RankAnomalies=" + rankAnomalies));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4){
            System.err.println("Usage: Job3Anomaly <input1> <input2> <output> <reducers>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Job3Anomaly");
        job.setJarByClass(Job3Anomaly.class);
        job.getConfiguration().setInt("min.reviews.perfect", 100);
        job.getConfiguration().setDouble("min.helpful.ratio", 0.2);
        job.getConfiguration().setDouble("min.verified.ratio", 0.5);
        job.setMapperClass(AnomalyMapper.class);
        job.setReducerClass(AnomalyReducer.class);

        int reducers = Integer.parseInt(otherArgs[3]);
        job.setNumReduceTasks(reducers);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[2]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}