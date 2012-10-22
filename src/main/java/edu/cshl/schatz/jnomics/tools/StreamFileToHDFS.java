package edu.cshl.schatz.jnomics.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * User: james
 */
public class StreamFileToHDFS {

    private static int BUFFER_SIZE = 10024;
    
    public static void main(String []args) throws IOException {
        if(args.length != 1){
            System.out.println("StreamFileToHdfs <output>");
            return;
        }
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream out = fs.create(new Path(args[0]));

        long total_read = 0;
        int read = 0;
        byte []buffer = new byte[BUFFER_SIZE];
        while((read = System.in.read(buffer)) > 0){
            out.write(buffer,0,read);
            total_read += read;
        }
        System.err.println("Total Read: " + total_read);
        out.close();
    }
}
