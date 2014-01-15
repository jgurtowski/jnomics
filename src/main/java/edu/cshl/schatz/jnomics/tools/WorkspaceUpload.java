package edu.cshl.schatz.jnomics.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;	 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class WorkspaceUpload {
	
			Configuration conf;
			
			public WorkspaceUpload(Configuration conf){
				this.conf =conf;
			}
		 
		    public void uploadExpression(FileSystem fs , Configuration conf) throws IOException {
		    	
		    	InputStream in = null;
		    	OutputStream out = null;
		    	ObjectMapper objectMapper = new ObjectMapper();
		    	
		    	String input = conf.get("grid.input.dir","");
		    	String kb_id = conf.get("kb_id","");
		    	String genome_id = conf.get("genome_id","");
		    	String onto_term_id = conf.get("onto_term_id","");
		    	String onto_term_name = conf.get("onto_term_name","");
		    	String onto_term_def = conf.get("onto_term_def","");
		    	String seq_type = conf.get("sequence_type","");
		    	String ref_genome = conf.get("reference","");
		    	String name = input.substring(input.lastIndexOf("/")+1);
//		    	 System.out.println("Input is "+ input );
//		    	 System.out.println("kb_id is "+ kb_id);
//		    	 System.out.println("genome_id is" + genome_id);
//		    	 System.out.println("onto_term_id is " + onto_term_id);
//		    	 System.out.println("onto_term_def is " + onto_term_def);
//		    	 System.out.println("sequence_type is " +  seq_type);
//		    	 System.out.println("ref is " + ref_genome);
				//String input = "/bluearc/home/schatz/sramakri/RNA-Seq/Tuxedo/challenge2/ct1_thout/transcripts.gtf";
		    	try{
		    	out = fs.create(new Path(kb_id));
		    //	out = fs.create(new Path("sample_test."+name));
			    in = fs.open(new Path(input));
		        
		        ExpressionSample expsamp = createExprSample(kb_id, in,genome_id,onto_term_id,onto_term_def,onto_term_name,seq_type,ref_genome);
		        
		        //configure Object mapper for pretty print
		        objectMapper.configure(SerializationFeature.INDENT_OUTPUT,false);
		         
		        //writing to console, can write to any output stream such as file
		        //StringWriter strexpsample = new StringWriter();
		       // ObjectWriter obj = new ObjectWriter();
		        objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
		        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
		        objectMapper.writeValue(out, expsamp);
		    	}catch(Exception e){
		    		e.printStackTrace();
		    	}finally{
		       // System.out.println("ExpressionSample JSON is\n"+strexpsample);
		        in.close();
		        out.close();
		    	}
				//return strexpsample.toString();
		    }
		     
		    public static ExpressionSample createExprSample(String kb_id,InputStream in,String genome_id, String term_id,String term_def,String term_name,String Seq_type , String ref_genome) {
		 
		   	// System.out.println("Inside ExpressionSample");
		        ExpressionSample expsample = new ExpressionSample();
		        expsample.setKbId(kb_id);
		        expsample.setSourceId(kb_id);
		        expsample.setSampleType("RNA-Seq");
		        expsample.setInterpretation("FPKM");
		        expsample.setDescription("");
		        expsample.setTitle("");
//		        expsample.setDataQuality(0);
//		        expsample.setOMedian((float) 0);
		        expsample.setExtSrcDate("");
		        
		        //ExpressionLevel explvl = new ExpressionLevel();
		        //explvl.setExprlevel(in);
		        expsample.setExprlevel(in);
		        
		        expsample.setGenomeId(genome_id);
     
		        ExpressionOntologyTerm expOnto = new ExpressionOntologyTerm();
		        expOnto.setTermId(term_id);
		        expOnto.setTermdef(term_def);
		        expOnto.setTermName(term_name);
		        expsample.setExpOntology(expOnto); 
//		        expsample.setExpPlatId(null);
//		        expsample.setExpSampleId(null);
//		        
//		        Protocol pro = new Protocol();
//		        pro.setName("RNA-Seq");
//		        pro.setDescription(Seq_type);
//		        expsample.setProtocol(pro);
//		        
		        Strain str = new Strain();
		        str.setGenomeId(ref_genome);
		        str.setDescription(ref_genome + " wild_type reference strain");
		        str.setName(ref_genome);
		        str.setRefStrain("Y");
		        str.setWildType("Y");
		        expsample.setStrain(str);
//		        
//		        //Persons per = new Persons();
//		        expsample.setPersons(null);
//		        
//		        expsample.setMolecule(null);
//		        expsample.setDataSource(null);
		        return expsample;
		    }
		 
}

