package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.writables.ReviewStatsWritable;

public class Job1Aggregation
{
    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, ReviewStatsWritable>
    {
        private final Text outputKey = new Text();

        private final ReviewStatsWritable outputValue = new ReviewStatsWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();

            if (line == null || line.length() == 0)
                return;

            String[] fields = line.split("\t"); //split with \t since the file is a .tsv file

            if (fields[0].equals("customer_id")) //skip header
                return;

            if (fields.length < 9)
                return;

            try {
                String productParent = fields[2];
                String productTitle = fields[3];
                String category = fields[4];
                double rating = Double.parseDouble(fields[5]);
                int helpfulVotes = Integer.parseInt(fields[6]);
                int totalVotes = Integer.parseInt(fields[7]);
                int verified = fields[8].equals("Y") ? 1 : 0; //we will use it for anomaly detection 

                outputKey.set(productParent);
                outputValue.set(1, rating, verified, helpfulVotes, totalVotes, category, productTitle);

                context.write(outputKey, outputValue);
            } catch(Exception e) {
                //skip malformed lines
            }
        }
    }

    public static class ReviewCombiner extends Reducer<Text, ReviewStatsWritable, Text, ReviewStatsWritable>
    {
        private final ReviewStatsWritable result = new ReviewStatsWritable();

        @Override
        public void reduce(Text key, Iterable<ReviewStatsWritable> values, Context context) throws IOException, InterruptedException
        {
            int reviewCount = 0;
            double ratingSum = 0.0;
            int verifiedCount = 0;
            int helpfulVotes = 0;
            int totalVotes = 0;
            String category = "";
            String productTitle = "";

            for (ReviewStatsWritable val : values) {
                reviewCount += val.getReviewCount();
                ratingSum += val.getRatingSum();
                verifiedCount += val.getVerifiedCount();
                helpfulVotes += val.getHelpfulVotes();
                totalVotes += val.getTotalVotes();
                category = val.getCategory();
                productTitle = val.getProductTitle();
            }

            result.set(reviewCount, ratingSum, verifiedCount, helpfulVotes, totalVotes, category, productTitle);

            context.write(key, result);
        }
    }

    public static class ReviewReducer extends Reducer<Text, ReviewStatsWritable, Text, Text>
    {
        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<ReviewStatsWritable> values, Context context) throws IOException, InterruptedException
        {
            int reviewCount = 0;
            double ratingSum = 0.0;
            int verifiedCount = 0;
            int helpfulVotes = 0;
            int totalVotes = 0;
            String category = "";
            String productTitle = "";

            for (ReviewStatsWritable val : values) {
                reviewCount += val.getReviewCount();
                ratingSum += val.getRatingSum();
                verifiedCount += val.getVerifiedCount();
                helpfulVotes += val.getHelpfulVotes();
                totalVotes += val.getTotalVotes();
                category = val.getCategory();
                productTitle = val.getProductTitle();
            }

            double avgRating = ratingSum / reviewCount;

            outputValue.set(productTitle + "###" + category + "###" + reviewCount + "###" + avgRating + "###" + verifiedCount + "###" + helpfulVotes + "###" + totalVotes); //we use ### to split the output since commas appear in the product_title 

            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Job1Aggregation <input> <output> <reducers>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Job1Aggregation");
        job.setJarByClass(Job1Aggregation.class);
        job.setMapperClass(ReviewMapper.class);
        job.setCombinerClass(ReviewCombiner.class);
        job.setReducerClass(ReviewReducer.class);
       
        int reducers = Integer.parseInt(otherArgs[2]);
        job.setNumReduceTasks(reducers);

        //mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ReviewStatsWritable.class);

        //reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //input output
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}