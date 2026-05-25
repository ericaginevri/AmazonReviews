package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CategoryScoreKey implements  WritableComparable<CategoryScoreKey>{
    private String category;
    private double score;
    
    public CategoryScoreKey(){}

    public CategoryScoreKey(final String category, final double score)
    {
        this.set(category, score);
    }

    public void set(final String category, final double score)
    {
        this.category = category;
        this.score = score;
    }

    public String getCategory() { return this.category; }
    public double getScore() { return this.score; }

    public void readFields(DataInput in) throws IOException
    {
        this.category = in.readUTF();
        this.score = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(this.category);
        out.writeDouble(this.score);
    }

    @Override
    public int compareTo(CategoryScoreKey that)
    {
        if (this == that)
            return 0;

        int categoryComparison = this.category.compareTo(that.category);

        if (categoryComparison != 0)
            return categoryComparison;

        if (this.score > that.score)
            return -1;
        if (this.score < that.score)
            return 1;

        return 0;
    }

    //for debug purpouses
    @Override
    public String toString()
    {
        return "(" + category + ", " + score + ")";
    }
}
