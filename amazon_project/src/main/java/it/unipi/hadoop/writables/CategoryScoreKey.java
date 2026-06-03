package it.unipi.hadoop.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CategoryScoreKey implements  WritableComparable<CategoryScoreKey>{
    private String category;
    private double score;
    
    //costruttore vuoto
    public CategoryScoreKey(){}

    //costruttore
    public CategoryScoreKey(final String category, final double score)
    {
        this.set(category, score);
    }

    //setter
    public void set(final String category, final double score)
    {
        this.category = category;
        this.score = score;
    }

    //getter
    public String getCategory() { return this.category; }
    public double getScore() { return this.score; }

    //deserializzare l'oggetto
    @Override
    public void readFields(DataInput in) throws IOException
    {
        this.category = in.readUTF();
        this.score = in.readDouble();
    }

    //serializzare l'oggetto
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(this.category);
        out.writeDouble(this.score);
    }

    //usata dalla fase di shuffle and sort per ordinare prima per categoria e poi per score discendente
    @Override
    public int compareTo(CategoryScoreKey that)
    {
        if (this == that)  //sono uguali
            return 0;

        int categoryComparison = this.category.compareTo(that.category); //comparison tra le stringhe category

        if (categoryComparison != 0) //se le categorie sono diverse fine
            return categoryComparison;

        //altrimenti comparison per lo score così ordino per score dentro la categoria
        if (this.score > that.score)
            return -1;
        if (this.score < that.score)
            return 1;

        return 0;
    }

    //rappresentazione in stringa -> può servire per debug
    @Override
    public String toString()
    {
        return "(" + category + ", " + score + ")";
    }
}
