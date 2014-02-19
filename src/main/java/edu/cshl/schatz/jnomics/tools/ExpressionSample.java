package edu.cshl.schatz.jnomics.tools;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

//import org.apache.commons.collections4.MultiMap;
//import org.apache.commons.collections4.map.MultiValueMap;




import com.fasterxml.jackson.annotation.*;

public class ExpressionSample {

		/**
	 * 
	 */
//	private static final long serialVersionUID = 1L;

		@JsonProperty("id")
	 	private String kb_id;
		
		@JsonProperty("source_id")
	    private String source_id;
		
		@JsonProperty("type")
	    private String type;
	    
		@JsonProperty("numerical_interpretation")
		private String numerical_interpretation;
		
		@JsonProperty("description")
	    private String description;
		
		@JsonProperty("title")
	    private String title;
		
		@JsonProperty("external_source_date")
	    private String external_source_date;
		
		@JsonProperty("expression_levels")
	    private Map<String, Double> expression_levels;
		
		@JsonProperty("genome_id")
	    private String genome_id;
		
		@JsonProperty("expression_ontology_terms")
	    private ExpressionOntologyTerm[] expression_ontology_terms;
		
		@JsonProperty("strain")
	    private Strain strain;

		public String getKbId() {
	        return kb_id;
	    }
	    public void setKbId(String kb_id) {
	        this.kb_id = kb_id;
	    }
	    public String getSourceId() {
	        return source_id;
	    }
	    public void setSourceId(String source_id) {
	        this.source_id = source_id;
	    }
	    public String getSampleType() {
	        return type;
	    }
	    public void setSampleType(String sample_type) {
	        this.type = sample_type;
	    }
	    public String getInterpretation() {
	        return numerical_interpretation;
	    }
	    public void setInterpretation(String num_intpre) {
	        this.numerical_interpretation = num_intpre;
	    }
	    public String getDescription() {
	        return description;
	    }
	    public void setDescription(String description) {
	        this.description = description;
	    }
	    public String getTitle() {
	        return title;
	    }
	    public void setTitle(String title) {
	        this.title = title;
	    }
	    public String getExtSrcDate() {
	        return external_source_date;
	    }
	    public void setExtSrcDate(String ext_src_date) {
	        this.external_source_date = ext_src_date;
	    }
	    
	    public Map<String,Double> getExprlevel(){
	    	return expression_levels;
	    }
	    public void setExprlevel(InputStream in){
	    	BufferedReader bfr = new BufferedReader(new InputStreamReader(in));
	    	String line;
	    	String feature, value;
	    	//String chr,feature , startpt, endpt , value; 
	    	Double fpkm = (double) 0;
		Pattern pattern = Pattern.compile("FPKM");
	    	expression_levels =  new HashMap<String, Double>();
	    	try {
				while((line = bfr.readLine()) != null){
					String[] columns = line.split("\t" ,-1);
					feature = columns[0];
				    //value = columns[9];
					value = columns[4];
				    String[] attr = pattern.split(value);
                    String[] fpkmval = attr[1].split("\"");
                    fpkm = Double.parseDouble(fpkmval[1].replaceAll("[\";]", ""));
                    fpkm = expression_levels.containsKey(feature) ? ( expression_levels.get(feature) + fpkm ): fpkm;
//                    if(expression_levels.containsKey(feature)){	
//                    	expression_levels.get(feature).;
//                    }else {
				    	//System.out.println("fpkm is " + fpkm);
				    //fpkm = Double.parseDouble(attr[9].replaceAll("[\";]", ""));
					//exprlevelMap.put(chr+":"+feature + ":" + startpt + ":" + endpt,fpkm);
                    expression_levels.put(feature,fpkm);
//                    }
				}
		   } catch (IOException e) {
				e.printStackTrace();
		   }
	    	
	    }
//	    public void setExplevel(Map<String,Double> exprlevel ) {
//	        this.expression_levels = exprlevel;
//	    }
	    
	    public ExpressionOntologyTerm[] getExpOntologyList(){
	    	return expression_ontology_terms;
	    }
	    public void setExpOntology(ExpressionOntologyTerm[] exprOnto ) {
	        this.expression_ontology_terms = exprOnto;
	    }
	    public String getGenomeId(){
	    	return genome_id;
	    }
	    public void setGenomeId(String genome_id ) {
	        this.genome_id = genome_id;
	    }
	    public Strain getStrain(){
	    	return strain;
	    }
	    public void setStrain(Strain strain ) {
	        this.strain = strain;
	    }

//	    public static ExpressionSample createExprSample(String kb_id,InputStream in,String genome_id, String term_id,String term_def,String term_name,String Seq_type , String ref_genome) {
//			 
//	        ExpressionSample expsample = new ExpressionSample();
//	        expsample.setKbId(kb_id);
//	        expsample.setSourceId(kb_id);
//	        expsample.setSampleType("RNASeq");
//	        expsample.setInterpretation("FPKM");
//	        expsample.setDescription("");
//	        expsample.setTitle("");
//	        expsample.setDataQuality(0);
//	        expsample.setOMedian((float) 0);
//	        expsample.setExtSrcDate("");
//	        
//	        ExpressionLevel explvl = new ExpressionLevel();
//	        explvl.setExprlevel(in);
//	        expsample.setExplevel(explvl);
//	        
//	        expsample.setGenomeId(genome_id);
//	        
//	        ExpressionOntology expOnto = new ExpressionOntology();
//	        expOnto.setTermId(term_id);
//	        expOnto.setTermdef(term_def);
//	        expOnto.setTermName(term_name);
//	        expsample.setExpOntology(expOnto); 
//
//	        expsample.setExpPlatId(null);
//	        expsample.setExpSampleId(null);
//	        
//	        Protocol pro = new Protocol();
//	        pro.setName("RNA-Seq");
//	        pro.setDescription(Seq_type);
//	        expsample.setProtocol(pro);
//	        
//	        Strain str = new Strain();
//	        str.setGenomeId(genome_id);
//	        str.setDescription(null);
//	        str.setName(ref_genome);
//	        str.setRefStrain(ref_genome);
//	        str.setWildType(null);
//	        expsample.setStrain(str);
//	        
//	        expsample.setPersons(null);
//	        expsample.setMolecule(null);
//	        expsample.setDataSource(null);
//
//
//	        return expsample;
//	    }

}
