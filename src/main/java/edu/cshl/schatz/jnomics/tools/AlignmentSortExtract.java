package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.ob.SAMRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

/**
 * User: james
 */
public class AlignmentSortExtract {

    public static void extract(Path in, Configuration conf, PrintStream out) throws IOException {

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(in);

        if(null == stats){
            throw new IOException("Could not find alignments at path: "+ in);
        }

        /** Parse the header for reference ordering **/
        List<String> seqList = new ArrayList<String>();
        SAMRecordWritable record = new SAMRecordWritable();
        String name;
        String header = null;
        for(FileStatus stat: stats){
            name = stat.getPath().getName();
            if(!name.startsWith("_") && !name.startsWith("part-r") && !name.startsWith(AlignmentSortReduce.UNMAPPED)){
                SequenceFile.Reader reader = new SequenceFile.Reader(fs,stat.getPath(),conf);
                reader.next(record);
                header = record.getTextHeader().toString();
                for(String line: header.split("\n")){
                    if(line.startsWith("@SQ")){
                        seqList.add(line.split("\t")[1].split(":")[1]);
                    }
                }
                reader.close();
                break;
            }
        }

        /** Create a map of the file names**/
        Map<String,ArrayList<Integer>> seqMap = new HashMap<String,ArrayList<Integer>>();
        for(FileStatus stat: stats){
            name = stat.getPath().getName();
            if(!name.startsWith("_") && !name.startsWith("part-r")){
                String [] arr = name.split("-");
                if(!seqMap.containsKey(arr[0]))
                    seqMap.put(arr[0],new ArrayList<Integer>());
                seqMap.get(arr[0]).add(Integer.parseInt(arr[1]));
            }
        }

        /** Sort the filenames **/
        for(String key :seqMap.keySet()){
            Collections.sort(seqMap.get(key));
        }

        /** If the unmapped reads exist, add them to the end of the sorted reference list **/
        if(seqMap.containsKey(AlignmentSortReduce.UNMAPPED)){
            seqList.add(AlignmentSortReduce.UNMAPPED);
        }
        
        /** Actually Print the alignments out **/
        out.println(header);
        SequenceFile.Reader reader;
        for(String ref: seqList){
            if(!seqMap.containsKey(ref))
		continue;
	    for(int fileNum : seqMap.get(ref)){
                reader = new SequenceFile.Reader(fs,new Path(in,ref+"-"+fileNum),conf);
                while(reader.next(record)){
                    out.println(record);
                }
                reader.close();
            }
        }
    }

    public static void main(String []args) throws Exception{
        if(1 != args.length){
            throw new Exception("extract <indir>");
        }

        extract(new Path(args[0]),new Configuration(),System.out);
    }
}
