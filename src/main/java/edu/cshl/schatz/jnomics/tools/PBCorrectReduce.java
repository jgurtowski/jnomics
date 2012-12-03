package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * User: james
 */
public class PBCorrectReduce extends JnomicsReducer<Text,Text,Text,Text>{


    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return Text.class;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

    }
}
