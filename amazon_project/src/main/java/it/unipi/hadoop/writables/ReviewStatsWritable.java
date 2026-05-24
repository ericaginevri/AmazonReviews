package it.unipi.hadoop.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class ReviewStatsWritable implements Writable
{
    private int reviewCount;
    private double ratingSum;  //double
    private int verifiedCount;
    private int helpfulVotes;
    private int totalVotes;
    private String category;
    private String productTitle;

    public ReviewStatsWritable() {} //costruttore vuoto

    public ReviewStatsWritable(int reviewCount, double ratingSum, int verifiedCount, int helpfulVotes, int totalVotes, String category, String productTitle)
    {
        this.reviewCount = reviewCount;
        this.ratingSum = ratingSum;
        this.verifiedCount = verifiedCount;
        this.helpfulVotes = helpfulVotes;
        this.totalVotes = totalVotes;
        this.category = category;
        this.productTitle = productTitle;
    }

    public int getReviewCount() { return reviewCount; }
    public double getRatingSum() { return ratingSum; }
    public int getVerifiedCount() { return verifiedCount; }
    public int getHelpfulVotes() { return helpfulVotes; }
    public int getTotalVotes() { return totalVotes; }
    public String getCategory() { return category; }
    public String getProductTitle() { return productTitle; }

    public void setReviewCount(int reviewCount) { this.reviewCount = reviewCount; }
    public void setRatingSum(double ratingSum) { this.ratingSum = ratingSum; }
    public void setVerifiedCount(int verifiedCount) { this.verifiedCount = verifiedCount; }
    public void setHelpfulVotes(int helpfulVotes) { this.helpfulVotes = helpfulVotes; }
    public void setTotalVotes(int totalVotes) { this.totalVotes = totalVotes; }
    public void setCategory(String category) { this.category = category; }
    public void setProductTitle(String productTitle) { this.productTitle = productTitle; }

    public void set(int reviewCount, double ratingSum, int verifiedCount, int helpfulVotes, int totalVotes, String category, String productTitle)
    {
        this.reviewCount = reviewCount;
        this.ratingSum = ratingSum;
        this.verifiedCount = verifiedCount;
        this.helpfulVotes = helpfulVotes;
        this.totalVotes = totalVotes;
        this.category = category;
        this.productTitle = productTitle;
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        reviewCount = in.readInt();
        ratingSum = in.readDouble();
        verifiedCount = in.readInt();
        helpfulVotes = in.readInt();
        totalVotes = in.readInt();
        category = in.readUTF();
        productTitle = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(reviewCount);
        out.writeDouble(ratingSum);
        out.writeInt(verifiedCount);
        out.writeInt(helpfulVotes);
        out.writeInt(totalVotes);
        out.writeUTF(category);
        out.writeUTF(productTitle);
    }

    @Override
    public String toString() {
        return productTitle + "," +
           category + "," +
           reviewCount + "," +
           ratingSum + "," +
           verifiedCount + "," +
           helpfulVotes + "," +
           totalVotes;
    }
}