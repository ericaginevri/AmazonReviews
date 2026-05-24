package it.unipi.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

public class ReviewStatsWritable implements Writable
{
    private int reviewCount;
    private double ratingSum;  //double
    private int verifiedCount;
    private int helpfulVotes;
    private int totalVotes;
    private Text category;

    public ReviewStatsWritable() {}

     public ReviewStatsWritable(int reviewCount, double ratingSum, int verifiedCount, int helpfulVotes, int totalVotes, Text category)
    {
        this.reviewCount = reviewCount;
        this.ratingSum = ratingSum;
        this.verifiedCount = verifiedCount;
        this.helpfulVotes = helpfulVotes;
        this.totalVotes = totalVotes;
        this.category = category;
    }

    public int getReviewCount() { return reviewCount; }
    public double getRatingSum()   { return ratingSum; }
    public int getVerifiedCount() { return verifiedCount; }
    public int getHelpfulVotes() { return helpfulVotes; }
    public int getTotalVotes() { return totalVotes; }
    public Text getCategory() { return category; }

    public void setReviewCount(int reviewCount) { this.reviewCount = reviewCount; }
    public void setRatingSum(double ratingSum)   { this.ratingSum = ratingSum; }
    public void setVerifiedCount(int verifiedCount) { this.verifiedCount = verifiedCount; }
    public void setHelpfulVotes(int helpfulVotes) { this.helpfulVotes = helpfulVotes; }
    public void setTotalVotes(int totalVotes) { this.totalVotes = totalVotes; }
    public void setCategory(Text category) { this.category = category; }

    @Override
    public void readFields(DataInput in) throws IOException{
        reviewCount=in.readInt();
        ratingSum=in.readDouble();
        verifiedCount=in.readInt();
        helpfulVotes=in.readInt();
        totalVotes=in.readInt();
        category.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(reviewCount);
        out.writeDouble(ratingSum);
        out.writeInt(verifiedCount);
        out.writeInt(helpfulVotes);
        out.writeInt(totalVotes);
        category.write(out);
    }

    @Override
    public String toString() {
        return reviewCount + "," +
            ratingSum + "," +
            verifiedCount + "," +
            helpfulVotes + "," +
            totalVotes + "," +
            category;
    }
}
