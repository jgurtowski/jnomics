package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.stream.FileImageInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import edu.cshl.schatz.jnomics.cli.ExtendedGnuParser;
import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.grid.JnomicsGridJobBuilder;
//import edu.cshl.schatz.jnomics.manager.client.ClientFunctionHandler;
//import edu.cshl.schatz.jnomics.manager.client.Compute;
//import edu.cshl.schatz.jnomics.manager.client.FS;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJobBuilder;

public class GridJobMain extends Configured implements Tool {


	//	public GridJobMain(Configuration conf){
	//		this.conf = conf;
	//	}
	//private static String workingdir = System.getProperty("user.home") + "/workingdir";
//	private static String workingdir = "/tmp";

	private static String printUsage = "Invalid Argument!  Program will now Exit... ";
	private static final Map<String,Class> gridClasses =
			new HashMap<String,Class>(){
		{
			put("tophat",alignTophat.class);
			put("Cufflinks",CufflinksSuite.class);
		}
	};

	public static void main(String[]  args) throws Exception{
		//System.out.println("I m entering this main");
		String jobname = args[0].substring(args[0].lastIndexOf(":") + 1);
		System.out.println(" args is " + args[0] +" jobname is  " + jobname );
		Configuration conf = new Configuration();
		FileSystem fs1 = null;
		File conffile = new File(System.getProperty("user.home")+"/" + jobname + ".xml");
		if (conffile.canRead()){
			System.out.println("Can read the file " + conffile);
		}
		conf.addResource(new Path(conffile.getAbsolutePath()));
		String gridJob = conf.get("grid.job.name");
		String[] jobparts =  conf.get("grid.job.name").split("-");
		String username = jobparts[0];
		
		try{
			URI hdfs_uri = new URI(conf.get("fs.default.name"));
			fs1 = FileSystem.get(hdfs_uri,conf,username);
			if(gridJob.matches(".*-tophat-.*")){
				System.out.println("I m entering this if loop tophat");
				alignTophat tophat = new alignTophat(conf);
				tophat.Preparebinaries(fs1, conf);
				tophat.align(fs1, conf);
				
			}else { 
				System.out.println("I m entering this if loop Cufflinks");
				CufflinksSuite cuff = new CufflinksSuite(conf);
				cuff.Preparebinaries(fs1, conf);
				if(gridJob.matches(".*-cufflinks-.*")){
					cuff.callCufflinks(fs1,conf);
				}else if(gridJob.matches(".*-cuffmerge-.*")){
					cuff.callCuffmerge(fs1,conf);
				}else if(gridJob.matches(".*-cuffdiff-.*")){
					cuff.callCuffdiff(fs1,conf);
				}else if(gridJob.matches(".*-cuffcompare-.*")){
					cuff.callCuffcompare(fs1,conf);
				}
			}
		}catch(Exception e) {
			throw new Exception(e.toString());
		}finally{
			fs1.close();
			conffile.delete();
		}
//			System.out.println("I m entering this if loop Cuffmerge");
//			FileSystem fs1 = null;
//			String[] jobparts =  conf.get("grid.job.name").split("-");
//			String username = jobparts[0];
//			URI hdfs_uri = new URI(conf.get("fs.default.name"));
//			CufflinksSuite cuff = null;
//			try{
//				cuff = new CufflinksSuite(conf);
//				fs1 = FileSystem.get(hdfs_uri,conf,username);
//				System.out.println("the filesystem path is " + fs1.getName() +  "and home directory is "  + fs1.getHomeDirectory());
//				cuff.Preparebinaries(fs1, conf);
//				
//			}catch(Exception e){
//				throw new Exception(e.toString());
//			}finally{
//				fs1.close();
//			}
//		
//			FileSystem fs1 = null;
//			String[] jobparts =  conf.get("grid.job.name").split("-");
//			String username = jobparts[0];
//			System.out.println("I m entering this if loop cuffdiff");
//			URI hdfs_uri = new URI(conf.get("fs.default.name"));
//			CufflinksSuite cuff = new CufflinksSuite(conf);
//			try{
//				fs1 = FileSystem.get(hdfs_uri,conf,username);
//				cuff.Preparebinaries(fs1, conf);
//				
//			}catch(Exception e){
//				throw new Exception(e.toString());
//			}finally{
//				fs1.close();
//			}
//		}
//			FileSystem fs1 = null;
//			String[] jobparts =  conf.get("grid.job.name").split("-");
//			String username = jobparts[0];
//			System.out.println("I m entering this if loop cuffcompare");
//			URI hdfs_uri = new URI(conf.get("fs.default.name"));
//			CufflinksSuite cuff = new CufflinksSuite(conf);
//			try{
//				fs1 = FileSystem.get(hdfs_uri,conf,username);
//				cuff.Preparebinaries(fs1, conf);
//				
//			}catch(Exception e){
//				throw new Exception(e.toString());
//			}finally{
//				fs1.close();
//			}
		
		//		try {
		//           
		//            System.out.println(conf);
		//            System.out.println(conf.get("fs.default.name"));
		//            FileSystem fs = FileSystem.get(conf);
		//            System.out.println("URI is " + fs.getUri());
		//            user_hdfs_path = fs.getUri();
		//            //DistributedCache.addCacheArchive(new URI(user_hdfs_path+"user/sramakri/cufflinks.tar"), conf );
		//            //DistributedCache.addArchiveToClassPath(archive, conf, fs);
		//            //FileStatus[] stats = fs.listStatus(new Path(user_hdfs_path));
		//            System.out.println(System.getProperty("user.home"));  
		//            Path inpath = new Path(user_hdfs_path+"/user/sramakri/t1.1.fq");
		//            System.out.println("Inpath is "+ inpath);
		//            String inpathstr = inpath.toString();
		//            Path outpath = new Path("/bluearc/home/schatz/sramakri/tophatout1");
		//            String outpathstr = outpath.toString(); 
		//            Path binary = new Path(user_hdfs_path+"/user/sramakri/tophat_v7.tar");
		//            String binarystr = binary.toString();
		//            Path pref_genome =  new Path(user_hdfs_path+"/user/sramakri/ecoli.fa");
		//            String ref_genome = pref_genome.toString();
		//            String ref = ref_genome.substring(ref_genome.lastIndexOf(".")+1);
		//            String cufflinks_opt = "-p 8";
		//            if(!fs.exists(inpath) || !fs.exists(binary) || !fs.exists(pref_genome)) {
		//            	return ;
		//            }else if(fs.exists(outpath)){
		//            	fs.delete(outpath);
		//            	System.out.println("Output path exists hence deleted");
		//            }
		//            fs.close();
		//           

		//		}catch(Exception e){
		//			e.printStackTrace();
		//		}
	}
	@Override
	public int run(String[] args) throws Exception {
		//		Options options = new Options();
		//        options.addOption(OptionBuilder.withArgName("required").withLongOpt("in").isRequired(true).hasArg().create());
		//        options.addOption(OptionBuilder.withArgName("required").withLongOpt("out").isRequired(true).hasArg().create());
		//        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("archives").isRequired(false).hasArg().create());
		//        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("max_split_size").isRequired(false).hasArg().create());
		//        ExtendedGnuParser parser = new ExtendedGnuParser(true);
		//        HelpFormatter formatter = new HelpFormatter();
		//        CommandLine cli = null;
		//        try{
		//            cli = parser.parse(options,args,false);
		//        }catch(ParseException e){
		//            formatter.printHelp(e.toString(),options);
		//            return -1;
		//        }
		//        JnomicsGridJobBuilder builder =  null;
		//        
		//        /** Set input and output path**/
		//        builder.setInputPath(cli.getOptionValue("in"))
		//                .setOutputPath(cli.getOptionValue("out"));
		//        /**Add any archives to distributed cache, requires full uri (hdfs://namenode:port/...)**/
		//        if(cli.hasOption("archives")){
		//            String []archives = cli.getOptionValue("archives").split(",");
		//            for(String archive : archives){
		//                builder.addArchive(archive);
		//            }
		//        }
		//
		//        if(cli.hasOption("max_split_size")){
		//            builder.setMaxSplitSize(Integer.parseInt(cli.getOptionValue("max_split_size")));
		//        }
		////        /**get additional args from mapper and reducer selected**/
		////        for( JnomicsArgument arg: builder.getArgs() ){
		////            options.addOption(OptionBuilder.withArgName(arg.isRequired() ? "required" : "optional")
		////                    .withLongOpt(arg.getName()).isRequired(arg.isRequired()).hasArg().create());
		////        }
		//
		//        /**reparse the arguments with the additonal options**/
		//        try{
		//            cli = parser.parse(options,args);
		//        }catch(ParseException e){
		//            formatter.printHelp(e.toString(),options);
		//            return -1;
		//        }
		//
		////        for(JnomicsArgument arg: builder.getArgs()){
		////            if(cli.hasOption(arg.getName())){
		////                builder.setParam(arg.getName(),cli.getOptionValue(arg.getName()));
		////            }
		////        }


		return 0;
	}


}
