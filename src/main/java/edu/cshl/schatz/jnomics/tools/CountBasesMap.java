package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import edu.cshl.schatz.jnomics.ob.ReadWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: james
 */
public class CountBasesMap extends JnomicsMapper<ReadCollectionWritable,NullWritable,Text,LongWritable> {

    private final Text name = new Text();
    private final LongWritable countWritable = new LongWritable(1);
    private long count; 

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
        return new JnomicsArgument[0];
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return CountLongReduce.class;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        name.set("Bases Count:");
    }

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        count=0;
        for(ReadWritable read: key.getReads()){
            count += read.getSequence().getLength();
        }
        countWritable.set(count);
        context.write(name,countWritable);
    }
}
