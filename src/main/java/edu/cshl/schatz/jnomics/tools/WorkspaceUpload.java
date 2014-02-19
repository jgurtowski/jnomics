package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;	 

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import us.kbase.auth.AuthToken;
import us.kbase.common.service.UObject;
import us.kbase.workspace.SaveObjectParams;
import us.kbase.workspace.WorkspaceClient;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cshl.schatz.jnomics.util.FileUtil;

public class WorkspaceUpload {

	Configuration conf;
	private static String workingdir;
	private final static org.slf4j.Logger logger = LoggerFactory.getLogger(WorkspaceUpload.class);

	/**
	 * <p>Constructor for WorkspaceUpload </p>
	 * <pre>
	 * Initialize the WorkspaceUpload variables.
	 * </pre>
	 */
	public WorkspaceUpload(Configuration conf){
		this.conf =conf;
		WorkspaceUpload.workingdir = System.getProperty("user.dir");
		System.out.println("Workingdir in wsupload is " + workingdir);

	}

	/**
	 * <p>uploadExpression</p>
	 * <pre>
	 * uploads Expression objects to Workspace.
	 * </pre>
	 */
	public void uploadExpression(FileSystem fs , Configuration conf) throws IOException {

		InputStream in = null;
		OutputStream out = null;
		String entityfile ;
		WorkspaceClient wrc;
		List<String> genome = new ArrayList<String>();
		ObjectMapper objectMapper = new ObjectMapper();
		String input = conf.get("grid.input.dir","");
		String kb_id = "kb|"+conf.get("kb_id","");
		String genome_id = conf.get("genome_id","");
		String description = conf.get("description","");
		String title = conf.get("title","");
		String srcDate  = conf.get("ext_src_date","");
		String onto_term_id = conf.get("onto_term_id","");
		String onto_term_name = conf.get("onto_term_name","");
		String onto_term_def = conf.get("onto_term_def","");
		String seq_type = conf.get("sequence_type","");
		String ref_genome = conf.get("reference","");
		String cdmi_url = conf.get("cdmi-url","");
		String wsc_url = conf.get("workspace-url","");
		String tarfile = conf.get("bedtools_binary","");
		String scriptfile =  conf.get("bedtools_script","");
		String token = new String(Base64.decodeBase64(conf.get("auth-token", "")));
		String name = input.substring(input.lastIndexOf("/")+1);

		//Change the the production workspace name
		String ws_name = "sriworkspace";

		String type = "ExpressionServices.ExpressionSample";

		logger.info("Input is "+ input );
		logger.info("kb_id is "+ kb_id);
		logger.info("genome_id is" + genome_id);
		logger.info("onto_term_id is " + onto_term_id);
		logger.info("onto_term_def is " + onto_term_def);
		logger.info("sequence_type is " +  seq_type);
		logger.info("ref is " + ref_genome);
		logger.info("cdmi-url is " +  cdmi_url);
		logger.info("workspace-url is " +  wsc_url);
		logger.info("token is " +  token);
		logger.info("bedtools-tar" + tarfile);
		logger.info("scriptfile is " + scriptfile);

		genome.add("kb|g."+genome_id);
		String tarname = new Path(tarfile).getName();
		String ret;
		try{
			//			out = fs.create(new Path(kb_id));
			Path parentdir =  new Path(input).getParent();
			GetGenomeFeatures get1 = new GetGenomeFeatures();
			entityfile = get1.getfeatures(fs, cdmi_url, genome);
			fs.copyToLocalFile(new Path(tarfile), new Path(workingdir));
			FileUtil.untar(fs,tarname, workingdir);
			fs.copyToLocalFile(new Path(entityfile), new  Path(workingdir));
			fs.copyToLocalFile(new Path(input), new Path(workingdir));
			findfeatureOverlap.runbedtoolsScript(fs,scriptfile,entityfile,parentdir);
			in = fs.open(new Path(parentdir.toString()+"/kbase_transcripts.gtf"));
			ExpressionSample expsamp = createExprSample(kb_id, in,genome_id,description,title,srcDate,onto_term_id, onto_term_def,onto_term_name,seq_type,ref_genome);
			//configure Object mapper for pretty print
			objectMapper.configure(SerializationFeature.INDENT_OUTPUT,false);
			//writing to console, can write to any output stream such as file
			StringWriter strexpsample = new StringWriter();
			objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
			objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			System.out.println("Writing the Expression object " + kb_id);
			objectMapper.writeValue(strexpsample, expsamp);
			//			objectMapper.writeValue(out, expsamp);
			Workspace wrksc = new Workspace(wsc_url, token);
			wrc = wrksc.getWrcClient();
			ret = wrksc.wrcsaveObject(wrc, ws_name, type, kb_id, strexpsample.toString());
			if(ret != null){
				logger.info(ret + " Object saved to workspace");
			}

		}catch(Exception e){
			e.printStackTrace();
		}finally{
			in.close();
			out.close();
		}
	}
	/**
	 * <p>createExprSample</p>
	 * <pre>
	 * Creates the Expression JSON Objects.
	 * </pre>
	 */
	public static ExpressionSample createExprSample(String kb_id,InputStream in,String genome_id, String desc , String title ,String srcdate, String term_id,String term_def,String term_name,String Seq_type , String ref_genome) {

		System.out.println("Inside ExpressionSample");
		ExpressionSample expsample = new ExpressionSample();;
		ExpressionOntologyTerm[] ontolist;
		expsample.setKbId(kb_id);
		expsample.setSourceId(kb_id);
		expsample.setSampleType("RNA-Seq");
		expsample.setInterpretation("FPKM");
		expsample.setDescription(desc);
		expsample.setTitle(title);
		//		        expsample.setDataQuality(0);
		//		        expsample.setOMedian((float) 0);
		expsample.setExtSrcDate(srcdate);
		expsample.setExprlevel(in);
		expsample.setGenomeId("kb|g."+genome_id); 	
		List<String> ontoIds = Arrays.asList(term_id.split(","));
		ontolist = new ExpressionOntologyTerm[ontoIds.size()];
		List<String> termDefs = Arrays.asList(term_def.split(","));
		List<String> termNames = Arrays.asList(term_name.split(","));
		for(int i=0; i < ontoIds.size(); i++){
			ontolist[i] = new ExpressionOntologyTerm();
			ontolist[i].setTermId(ontoIds.get(i));
			ontolist[i].setTermdef(termDefs.get(i));
			ontolist[i].setTermName(termNames.get(i));
		}
		expsample.setExpOntology(ontolist);		        
		Strain str = new Strain();
		str.setGenomeId("kb|g." + ref_genome);
		str.setDescription("kb|g." +  ref_genome + " wild_type reference strain");
		str.setName("kb|g." + ref_genome);
		str.setRefStrain("Y");
		str.setWildType("Y");
		expsample.setStrain(str);

		return expsample;
	}

}

