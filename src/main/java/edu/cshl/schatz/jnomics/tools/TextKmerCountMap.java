package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.util.Nucleotide;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * User: james
 */
public class TextKmerCountMap extends JnomicsMapper<LongWritable, Text, Text, LongWritable>{

    private JnomicsArgument ksize_arg = new JnomicsArgument("ksize","Kmer Size",true);
    private JnomicsArgument read_column_arg = new JnomicsArgument("read_column",
            "Column in text file that contains the read (0 indexed)", true);

    private int ksize;
    private int read_column;
    
    private Text textKmer = new Text();
    private LongWritable one = new LongWritable(1);

    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return LongWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{ksize_arg,read_column_arg};
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ksize = conf.getInt(ksize_arg.getName(),0);
        read_column = conf.getInt(read_column_arg.getName(),0);
        if(ksize <= 0)
            throw new IOException("Bad Kmer size");
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return TextKmerCountReduce.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormat() {
        return TextInputFormat.class;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String []arr = value.toString().split("\t");
        String read = arr[read_column];
        String kmer, kmer_rev;
        for(int i=0;i<read.length()-ksize+1; ++i){
            kmer = read.substring(i,i+ksize);
            kmer_rev = Nucleotide.reverseComplement(kmer);
            //take lexigraphically lower kmer
            textKmer.set(kmer.compareTo(kmer_rev) < 0 ? kmer : kmer_rev);
            context.write(textKmer, one);
        }
    }
}
