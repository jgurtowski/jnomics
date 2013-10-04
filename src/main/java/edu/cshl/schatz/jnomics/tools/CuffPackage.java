package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.FileUtil;



//import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

/**
 * Author: Sri
 *
 * Distributed cufflinks
 */
public class CuffPackage{

	private File[] tmpFiles;
	private String cufflinks_cmd;
	private Process process;
	private static String workingdir = System.getProperty("user.dir");

	private final String[] cuff_bin = {"cufflinks","cuffmerge","cuffdiff","cuffcompare" };

	protected void setup(final Configuration conf,String cuff_input, String cufflinks_opts,
			String cufflinks_output_dir, String cufflinks_binary) throws IOException,InterruptedException {
		System.out.println("cufflinks opts are "+cufflinks_opts +" cufflinks output dir "+ cufflinks_output_dir +" cufflinks binary is "+cufflinks_binary);
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(cufflinks_binary);
		Path pinput = new Path(cuff_input);
		System.out.println("filename is " + p.getName());
		String binaryfile =  p.getName();
		String infile = pinput.getName();

		if(!fs.exists(p))
			throw new IOException(" Can't find cufflinks binary: " + cufflinks_binary);

		fs.copyToLocalFile(false,new Path(cufflinks_binary), new Path(workingdir));
		fs.copyToLocalFile(false,new Path(cuff_input), new Path(workingdir));

		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
			if(new File(cuff_bin[0]).isFile() && new File(cuff_bin[1]).isFile() 
					&& new File(cuff_bin[2]).isFile() && new File(cuff_bin[3]).isFile()) {
				System.out.println("Cufflinks binaries are loaded");

			}else {
				System.err.println("Error loading Cufflink binaries!");
				return;
			}
		}

		if(new File(cuff_input).isFile()){
			System.out.println("Input file  " + cuff_input+ " is loaded");
		}
		cufflinks_cmd =  String.format("%s %s -o %s %s",cuff_bin[0], cufflinks_opts,cufflinks_output_dir,infile);
		System.out.println("command is "+ cufflinks_cmd);
		Path path = new Path(workingdir+cufflinks_output_dir);
		File dir = new File(cufflinks_output_dir);
		dir.mkdir();

		if(dir.isDirectory()){
			System.out.println("Cufflinks Output directory "+ dir.toString() + " is setup");
		}
		fs.close();
		return;
	}

	public void align(final Configuration  conf, String cuff_input,String cufflinks_opts, 
			String cufflinks_output_dir, String cufflinks_binary) throws IOException, InterruptedException {
		System.out.println("Entered align. conf is" +  conf.toString());
		FileOutputStream fout = null;
		FileSystem  fs = FileSystem.get(conf);

		String hdfs_base = fs.getUri().toString();
		Path outdir = new Path(cufflinks_output_dir.substring(cufflinks_output_dir.lastIndexOf("/")+1));
		Path path = new Path(hdfs_base + "/user/sramakri/" + outdir );
		try{
			setup(conf, cuff_input, cufflinks_opts,  cufflinks_output_dir,  cufflinks_binary);
			Thread connecterr,connectout;
			System.out.println("launching alignment");

			String cmd = cufflinks_cmd;
			System.out.println("Command formed is " + cmd);
			// for(String cmd: isPairedEnd() ? aln_cmds_pair : aln_cmds_single){
			process = Runtime.getRuntime().exec(cmd);
			System.out.println(cmd);
			// Reattach stderr and write System.stdout to tmp file
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
							// conf.progress();
						}
					});

			System.out.println("hdfs output url path is " +  path);
			System.out.println("ouput dir path is " +  outdir);
			fout = new FileOutputStream(new File(cufflinks_output_dir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();


			//fs.create(path);
			fs.mkdirs(path);
			if(fs.isDirectory(path)){
				System.out.println("Output Copied to HDFS dir "+ outdir);
			}else{
				System.err.println("Error copying files to HDFS");				
			}

			fs.copyFromLocalFile(false,new Path(cufflinks_output_dir) , path);
			//org.apache.hadoop.fs.FileUtil.copy(fs, new Path(cufflinks_output_dir+"/*"),path,false,conf);
			if(fs.isDirectory(path)){
				System.out.println("Output Copied to HDFS dir "+ outdir);
			}else{
				System.err.println("Error copying files to HDFS");				
			}		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			//fout.close();
			fs.close();
		}
		//context.progress();
		// }
		System.out.println(" Cufflinks Process Complete  ");


	}

}