package edu.cshl.schatz.jnomics.tools;

import edu.cshl.schatz.jnomics.io.ThreadedStreamConnector;
import edu.cshl.schatz.jnomics.util.FileUtil;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;

import java.io.File;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// unused imports 
//import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
//import edu.cshl.schatz.jnomics.io.AlignmentReaderContextWriter;
//import edu.cshl.schatz.jnomics.util.Command;
//import edu.cshl.schatz.jnomics.util.InputStreamHandler;
//import edu.cshl.schatz.jnomics.util.OutputStreamHandler;
//import edu.cshl.schatz.jnomics.util.ProcessUtil;
//import org.apache.hadoop.fs.FileUtil;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.io.OutputStreamWriter;
//import java.net.URI;
//import java.net.URL;
//import java.io.BufferedWriter;
//import java.io.FileNotFoundException;
//import java.util.Collection;
//import java.util.Iterator;
//
//import java.util.ListIterator;
//import java.util.Properties;

/**
 * Author: Sri
 *
 * Distributed cufflinks
 */
public class CufflinksSuite{

	Configuration conf;

	private String cufflinks_cmd;
	private Process process;
	private static String workingdir;
	private static String jobdirname;
	private static String userhome ;
	private static String jobid  = null;
	private final String[] cuff_bin = {"cufflinks","cuffmerge","cuffdiff","cuffcompare" };

	public CufflinksSuite(Configuration conf) {
		this.conf = conf;
		try {
			CufflinksSuite.workingdir = new File(".").getCanonicalPath();
			jobdirname =  conf.get("grid.output.dir");
			userhome = System.getProperty("user.home");
			jobid = System.getenv("JOB_ID");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected boolean Preparebinaries(final FileSystem fs, final Configuration conf) throws IOException,InterruptedException {
		String cufflinks_binary = conf.get("cufflinks_binary");
		Path bin_path = new Path(cufflinks_binary);
		if(!fs.exists(bin_path))
			throw new IOException(" Can't find cufflinks binary: " + cufflinks_binary);
		String binaryfile =  bin_path.getName();
		fs.copyToLocalFile(false,new Path(cufflinks_binary), new Path(workingdir));
		if(new File(workingdir+"/"+binaryfile).isFile()){
			org.apache.hadoop.fs.FileUtil.unTar(new File(workingdir+"/"+binaryfile),new File(workingdir));
			if(new File(cuff_bin[0]).isFile() && new File(cuff_bin[1]).isFile() 
					&& new File(cuff_bin[2]).isFile() && new File(cuff_bin[3]).isFile()) {
				System.out.println("Cufflinks binaries are loaded");
			}else {
				System.err.println("Error loading Cufflinks binaries!");
				throw new IOException();
			}
		}
		return true;
	}

	public boolean callCufflinks(final FileSystem  fs, Configuration conf1) throws IOException, InterruptedException {
		String cuff_bam_input = conf.get("grid.input.dir");
		String infile = new Path(cuff_bam_input).getName();
		String cufflinks_opts =  conf.get("cufflinks_opts","");
		String cuff_output_dir = workingdir + "/" + conf.get("grid.output.dir","");
		FileOutputStream fout = null;
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString());
		File dir = new File(cuff_output_dir);
		try{
			fs.copyToLocalFile(false,new Path(cuff_bam_input), new Path(workingdir));
			if(new File(workingdir + "/" + cuff_bam_input).isFile()){
				System.out.println("Input file  : " + cuff_bam_input+ " ");
			}
			if(!dir.exists()){
				dir.mkdir();
			}
			if(dir.isDirectory()){
				System.out.println("Output directory : "+ dir.toString());
			}
			cufflinks_cmd =  String.format("%s/cufflinks %s -o %s %s/%s",workingdir, cufflinks_opts,cuff_output_dir,workingdir,infile);
			Thread connecterr,connectout;
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cufflinks cmd : "+ cmd);
			process = Runtime.getRuntime().exec(cmd);
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
						}
					});
			fout = new FileOutputStream(new File(cuff_output_dir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			if(!fs.exists(hdfs_job_path)) {
				fs.mkdirs(hdfs_job_path);	
			}
			fs.copyFromLocalFile(false,new Path(cuff_output_dir) , hdfs_job_path);
			File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-cufflinks-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				fs.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path );
			}
			//			if(fs.isDirectory(hdfs_job_path)){
			//				System.out.println("Output Copied to HDFS dir "+ hdfs_job_path);
			//			}else{
			//				System.err.println("Error copying files to HDFS");				
			//			}
			System.out.println(" Cufflinks Process is Complete  ");
		}catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public boolean callCuffmerge(final FileSystem fs,Configuration conf2) throws IOException, InterruptedException {
		String cuffmerge_output_dir = workingdir + "/" + conf2.get("grid.output.dir","");
		String cuffmerge_in = conf2.get("grid.input.dir","");
		String inputfile = new Path(cuffmerge_in).getName();
		String cuffmerge_opts = conf.get("cuffmerge_opts","");
		String ref_genome = conf.get("cuffmerge_ref_genome","");
		String ref_genome_local = new Path(ref_genome).getName();
		String gtf_file = conf.get("cuffmerge_gtf","");

		BufferedReader reader;
		File dir = new File(cuffmerge_output_dir);
		FileOutputStream fout = null;
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString());
		String line = null;
		List<String> filelist =  new ArrayList<String>();

		try {

			fs.copyToLocalFile(false,new Path(cuffmerge_in), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_genome), new Path(workingdir));
			if(new File(workingdir + "/" + inputfile).isFile()){
				System.out.println("Input file  :" + cuffmerge_in);
			}
			reader = new BufferedReader(new FileReader(new File(workingdir + "/" +inputfile)));
			while((line = reader.readLine()) != null){
				filelist.add(line);
			}
			if(!FileUtil.copyFromHdfs(fs, filelist, workingdir)){
				System.err.println("Error copying the input files ");
			}
			if(!dir.exists()){
				dir.mkdir();
			}
			if(!dir.isDirectory()){
				System.err.println("Error creating the output dir " + cuffmerge_output_dir);
			}
			cufflinks_cmd =  String.format("%s/cuffmerge %s -o %s -s %s/%s %s/%s",
					workingdir, cuffmerge_opts,cuffmerge_output_dir,workingdir,ref_genome_local,workingdir,inputfile);
			Thread connecterr,connectout;
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cuffmerge command :" + cmd);
			process = Runtime.getRuntime().exec(cmd);
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
							// conf.progress();
						}
					});
			fout = new FileOutputStream(new File(cuffmerge_output_dir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			if(!fs.exists(hdfs_job_path)) {
				fs.mkdirs(hdfs_job_path);	
			}
			fs.copyFromLocalFile(false,new Path(cuffmerge_output_dir) , hdfs_job_path);
			File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-cuffmerge-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				fs.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path );
			}
			//			if(fs.isDirectory(hdfs_job_path)){
			//				System.out.println("Output Copied to HDFS dir "+ hdfs_job_path);
			//			}else{
			//				System.err.println("Error copying files to HDFS");				
			//			}		
			System.out.println(" Cuffmerge Process is Complete  ");
		}catch(Exception e){
			e.printStackTrace();
		}

		return true;	

	}

	public boolean callCuffdiff(final FileSystem fs, Configuration conf) throws IOException, InterruptedException {
		String[] jobparts = conf.get("grid.job.name").split("-");
		String cuffdiff_input = conf.get("input_files","");
		final String cuffdiff_out = conf.get("grid.output.dir","");
		String ref_genome = conf.get("cuffdiff_ref_genome");
		String cuffdiff_opts = conf.get("cuffdiff_opts");
		String merged_gtf = conf.get("cuffdiff_merged_gtf");
		String cuffdiff_lbs = conf.get("cuffdiff_condn_labels");

		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString());
		File dir = new File(workingdir + "/" + cuffdiff_out);

		try {
			System.out.println("Inside try in cuffdiff ");
			List<String> cuffdiff_in = Arrays.asList(cuffdiff_input.split(","));
			if(!FileUtil.copyFromHdfs(fs,cuffdiff_in, workingdir)){
				System.err.println("Error in Copying the Cuffdiff Input files "); 
			}
			String ref = ref_genome.substring(ref_genome.lastIndexOf("/") + 1);
			String gtf = merged_gtf.substring(merged_gtf.lastIndexOf("/") + 1);
			fs.copyToLocalFile(false, new Path(merged_gtf), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_genome),  new Path(workingdir));

			FileOutputStream fout = null;
			if(!dir.exists()){
				dir.mkdir();
			}
			if(dir.isDirectory()){
				System.out.println("Output directory : "+ dir.toString());
			}
			cufflinks_cmd =  String.format("%s/cuffdiff -o %s -b %s/%s %s -L %s -u %s/%s",
					workingdir,cuffdiff_out,workingdir,ref,cuffdiff_opts,cuffdiff_lbs,workingdir,gtf);
			StringBuilder sb =  new StringBuilder();
			for(String bam_file : cuffdiff_in){
				sb.append(" ").append(workingdir).append("/").append(bam_file).append(",");
			}
			cufflinks_cmd = cufflinks_cmd.concat(sb.toString());
			System.out.println("Executing Cuffdiff command " + cufflinks_cmd);

			//			Command cuff_cmd = new Command(cufflinks_cmd,new InputStreamHandler() {
			//				
			//				@Override
			//				public void handle(InputStream in) {
			//					// TODO Auto-generated method stub
			//					
			//				}
			//			}}; ,new OutputStreamHandler(){
			//			
			//			@Override
			//			public void handle(OutputStream out) {
			//				try{
			//				out = new FileOutputStream(new File(workingdir + cuffdiff_out + "/log"));
			//				Thread connect = new Thread(new ThreadedStreamConnector(process.getInputStream(), out));
			//				}catch(Exception e) {
			//					System.err.println("Error Writing to FileOutputStream " +  e.toString());
			//				}
			//			}}); 
			//		
			//			ProcessUtil.runCommandEZ(cuff_cmd);

			Thread connecterr,connectout;
			String cmd = cufflinks_cmd;
			//System.out.println("Command formed is " + cmd);
			process = Runtime.getRuntime().exec(cmd);
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
						}
					});

			fout = new FileOutputStream(new File(workingdir + "/" + cuffdiff_out+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			if(!fs.exists(hdfs_job_path)) {
				fs.mkdirs(hdfs_job_path);	
			}
			fs.copyFromLocalFile(false, new Path(cuffdiff_out), hdfs_job_path);
			File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-cuffdiff-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				fs.copyFromLocalFile(true,new Path(file.getAbsolutePath()),hdfs_job_path);
			}
			//			if(fs.isDirectory(hdfs_job_path)){
			//				System.out.println("Output Copied to path "  +  hdfs_job_path);
			//			}else{
			//				System.err.println("Error copying files to HDFS");				
			//			}	
			System.out.println(" Cuffdiff Process is Complete  ");
		}catch(IOException e){
			throw new IOException(e.toString());
		}
		return true;	
	}
	public boolean callCuffcompare(final FileSystem fs,Configuration conf ) throws Exception {
		String cuffcompare_in = conf.get("grid.input.dir");
		String infile = new Path(cuffcompare_in).getName();
		String cuffcompare_opts = conf.get("cuffcompare_opts","");
		String ref_gtf = fs.getHomeDirectory().toString() + "/" +conf.get("cuffcompare_gtf","");
		String ref_gtfname = new Path(ref_gtf).getName();
		final String cuffcompare_out = conf.get("grid.output.dir","");
		Path hdfs_job_path = new Path(fs.getHomeDirectory().toString() + "/" + jobdirname);
		FileOutputStream fout = null;
		String line = null;
		List<String> filelist = new ArrayList<String>();

		boolean ret;
		try{
			fs.copyToLocalFile(false,new Path(cuffcompare_in), new Path(workingdir));
			fs.copyToLocalFile(false,new Path(ref_gtf), new Path(workingdir));
			if(new File(workingdir + "/" + infile).isFile()){
				System.out.println("Input file  : " + cuffcompare_in+ " ");
			}
			BufferedReader reader = new BufferedReader(new FileReader(new File(workingdir + "/" +infile)));
			while((line = reader.readLine()) != null){
				filelist.add(line);
			}
			if(!FileUtil.copyFromHdfs(fs, filelist, workingdir)){
				System.err.println("Error copying the input files");
			}
			cufflinks_cmd =  String.format("%s/cuffcompare -i %s/%s -o %s -r %s/%s",
					workingdir,workingdir,infile,cuffcompare_out,workingdir,ref_gtfname);
			Thread connecterr,connectout;
			String cmd = cufflinks_cmd;
			System.out.println("Executing Cuffcompare Command : " + cmd);
			process = Runtime.getRuntime().exec(cmd);
			connecterr = new Thread(
					new ThreadedStreamConnector(process.getErrorStream(), System.err){
						@Override
						public void progress() {
							// conf.progress();
						}
					});

			fout = new FileOutputStream(new File(workingdir+"/log"));
			connectout = new Thread(new ThreadedStreamConnector(process.getInputStream(),fout));
			connecterr.start();connectout.start();
			connecterr.join();connectout.join();
			process.waitFor();
			fout.close();
			if(!fs.exists(hdfs_job_path)) {
				fs.mkdirs(hdfs_job_path);	
			}
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".combined.gtf") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".loci") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".stats") , hdfs_job_path);
			fs.copyFromLocalFile(false,new Path( workingdir + "/" +cuffcompare_out+".tracking") , hdfs_job_path);
			File[] files = new File(userhome).listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.matches(".*-cuffcompare-.*[.]."+jobid);
				}
			});
			for(File file : files)
			{
				System.out.println("file is " + file.getAbsolutePath());
				fs.copyFromLocalFile(true,new Path(file.getAbsolutePath()),fs.getHomeDirectory());
			}
			//			if(fs.isDirectory(hdfs_job_path)){
			//				System.out.println("Output Copied to HDFS dir "+ hdfs_job_path);
			//			}else{
			//				System.err.println("Error copying files to HDFS");				
			//			}
			System.out.println(" Cuffcompare Process is Complete  ");
		}catch(Exception e){
			e.printStackTrace();
			//throw new Exception(e.toString());
		}
		return true;	
	}	
}

