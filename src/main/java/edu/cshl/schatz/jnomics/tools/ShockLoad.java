package edu.cshl.schatz.jnomics.tools;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import us.kbase.shock.client.BasicShockClient;
import us.kbase.shock.client.ShockNode;
import us.kbase.shock.client.exceptions.InvalidShockUrlException;

//import edu.cshl.schatz.jnomics.manager.api.JnomicsThriftException;
public class ShockLoad extends JnomicsMapper<LongWritable, Text,NullWritable,NullWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
			{
				System.err.println("value is " + value.toString());
				
				FSDataInputStream inStream = null;

				FileSystem fs = FileSystem.get(new Configuration());
				BasicShockClient base;
				Path path = new Path(value.toString());
				String filename = new File(value.toString()).getName();
				System.err.println("Filename is " + filename);
				inStream = fs.open(path);
				int  Length = (int)(fs.getLength(path));
				System.err.println("Length is" + Length);
				byte[] buf = new byte[Length];
				try{
				
					URL mshadoop = new URL("http://mshadoop1.cshl.edu:7546");
					System.err.println(mshadoop);
					base = new BasicShockClient(mshadoop);
					int i = 0;
					int total = 0;
					inStream.read(buf);
					while(-1 != (i = inStream.read(buf))){
						total += i;
						
						System.err.print("\r"+total+"/"+Length+" " + ((float)total)/Length * 100 + "%");    
					}
					ShockNode sn = base.addNode(buf, filename);
					System.err.println("Node id is "+ sn.getId().toString());
					//context.write(new Text("hello World"), new Text("1234"));
				}		
				catch(Exception e){
					System.out.println(e.toString());
					throw new IOException(e.getMessage());
				}
				inStream.close();
				fs.close();
			
			}
		
		}

		@Override
		public Class getOutputKeyClass() {
			return NullWritable.class;
		}

		@Override
		public Class getOutputValueClass() {
			return NullWritable.class;
		}
	}

