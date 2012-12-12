package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.FastaParser;
import edu.cshl.schatz.jnomics.io.MPStreamWriter;
import edu.cshl.schatz.jnomics.ob.SequencingRead;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * User: james
 */
public class FastaLoader {

    public static void main(String []args) throws IOException {
        
        if(2 != args.length){
            System.out.println("FastaLoader <in.fa> <hdfs_out>");
            System.exit(1);
        }

        FastaParser parser = new FastaParser(new FileInputStream(args[0]));
        FileSystem fs = FileSystem.get(new Configuration());

        String hdfsFile = args[1];
        Path p = new Path(hdfsFile);
        if(fs.exists(p)){
            System.out.println("File already exists : " + p);
            System.exit(1);
        }
        
        MPStreamWriter writer = new MPStreamWriter(fs.create(p));
        SequencingRead read = new SequencingRead();

        int i = 1;
        for(FastaParser.FastaRecord record: parser){
            read.name = record.getName();
            read.sequence = record.getSequence();
            writer.addRecord(read);
            if(0 == i % 1000000)
                System.err.println("Loaded: " + i);
            i++;
        }

        writer.close();
    }
}
