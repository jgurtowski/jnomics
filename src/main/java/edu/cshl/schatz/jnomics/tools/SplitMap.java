package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.ob.ReadCollectionWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SplitMap extends JnomicsMapper<ReadCollectionWritable,NullWritable,ReadCollectionWritable,NullWritable> {

    @Override
    protected void map(ReadCollectionWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    @Override
    public Class getOutputKeyClass() {
        return ReadCollectionWritable.class;
    }

    @Override
    public Class getOutputValueClass() {
        return NullWritable.class;
    }

    @Override
    public JnomicsArgument[] getArgs() {
        return new JnomicsArgument[]{};
    }
}