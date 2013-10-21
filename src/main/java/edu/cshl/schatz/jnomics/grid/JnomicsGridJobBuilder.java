package edu.cshl.schatz.jnomics.grid;

import org.apache.hadoop.conf.Configuration;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//unused imports
//import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
//import edu.cshl.schatz.jnomics.mapreduce.JnomicsJobBuilder;
//import edu.cshl.schatz.jnomics.util.TextUtil;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.ggf.drmaa.JobInfo;
//import org.ggf.drmaa.Version;
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.Collections;
//import java.util.Properties;


/**
 * User: Sri
 */
public class JnomicsGridJobBuilder {

	private Configuration conf = null;
	List<String> archives = new ArrayList<String>();
	public static Map<Integer,String> returnCode =  new HashMap<Integer, String>();

	public JnomicsGridJobBuilder(Configuration conf){
		this.conf = conf;
		returnCode.put(0, "UNDETERMINED");
		returnCode.put(16, "QUEUED_ACTIVE");
		returnCode.put(17, "SYSTEM_ON_HOLD");
		returnCode.put(18, "USER_ON_HOLD");
		returnCode.put(19, "USER_SYSTEM_ON_HOLD");
		returnCode.put(32, "RUNNING");
		returnCode.put(33, "SYSTEM_SUSPENDED");
		returnCode.put(34, "USER_SUSPENDED");
		returnCode.put(35, "USER_SYSTEM_SUSPENDED");
		returnCode.put(48, "DONE");
		returnCode.put(64, "FAILED");
	}

	public JnomicsGridJobBuilder setParam(String name, String value){	
		conf.set(name,value);
		return this;
	}
	public JnomicsGridJobBuilder setInputPath(String in){
		conf.set("grid.input.dir",in);
		return this;
	}

	public JnomicsGridJobBuilder setOutputPath(String out){
		conf.set("grid.output.dir", out);
		return this;
	}

	public JnomicsGridJobBuilder addConfModifiers(Map<String,String> modifiers){
		for(Map.Entry<String,String> entry: modifiers.entrySet()){
			conf.set(entry.getKey(),entry.getValue());
		}
		return this;
	} 

	public JnomicsGridJobBuilder setJobName(String name){
		conf.set("grid.job.name", name);
		return this;
	}  
	
	public JnomicsGridJobBuilder LaunchGridJob(Configuration conf) throws Exception{
		String scriptfile = new File( new File(".").getAbsolutePath() + "/safe_bin/GridJobLauncher.sh" ).getAbsolutePath();
		String workingdir = conf.get("grid_working_dir");
		String jobname = conf.get("grid.job.name");
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		JobTemplate jt = null;
		String jobId = null;
		try {
			session.init("");
			jt = session.createJobTemplate();
			jt.setRemoteCommand(scriptfile+" "+workingdir+":"+jobname);
			//jt.setArgs(Collections.singletonList("5"));
			//jt.setWorkingDirectory(workingdir);
			//jt.setErrorPath(":" + workingdir);
			//jt.setOutputPath(":" + workingdir);
			jt.setJobName(jobname);
			jobId = session.runJob(jt);
			System.out.println("Jobname is : " + jt.getJobName());
		}catch(Exception e){
			throw new Exception();
		}finally{
			session.exit();

		}
		conf.set("grid_jobId",jobId);
		return this ;
	}

	public String getjobstatus(String JobId) throws DrmaaException{
		SessionFactory factory = SessionFactory.getFactory();
		Session session = factory.getSession();
		int ret = 0;
		try {
			session.init("");
			ret  = session.getJobProgramStatus(JobId);

		} catch (DrmaaException e) {
			e.printStackTrace();
		}finally{
			session.exit();
		}
		return returnCode.get(ret);
	}

	private void ifNotSetConf(String key, String value){
		if(null == conf.get(key,null))
			conf.set(key,value);
	}

	public JnomicsGridJobBuilder setMaxSplitSize(int num){
		conf.setInt("mapred.max.split.size",num);
		return this;
	}

	/**Returns Arguments that are wanted by the mapper and/or reducer **/
	//    public JnomicsArgument[] getArgs() throws Exception{
	//        JnomicsMapper mapperInst = mapper.newInstance();
	//        JnomicsArgument[] args = mapperInst.getArgs();
	//        if(null != reducer){
	//            JnomicsReducer reducerInst = reducer.newInstance();
	//            args = ArrayUtils.addAll(args, reducerInst.getArgs());
	//        }
	//        return args;
	//    }

	public Configuration getJobConf() throws Exception {
		return this.conf;
	}


}
