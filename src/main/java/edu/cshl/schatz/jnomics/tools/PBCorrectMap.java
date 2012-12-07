package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * User: james
 */
public class PBCorrectMap extends JnomicsMapper<Text, NullWritable, Text, Text> {

    private Text read_name = new Text();
    private Text read_alignment = new Text();
    
    @Override
    public Class getOutputKeyClass() {
        return Text.class;
    }

    @Override
    public Class getOutputValueClass() {
        return Text.class;
    }


    @Override
    protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        String blast_tbl_output = key.toString();
        String[] arr = blast_tbl_output.split("\t");
        
        if(arr.length != 13){
            System.err.println("Expected 13 columns, got: "+ blast_tbl_output);
            return;
        }

        read_name.set(arr[1]);
        read_alignment.set(arr[8]+","+arr[9]+","+arr[12]);

        context.write(read_name,read_alignment);
    }
    
}
