package edu.cshl.schatz.jnomics.tools;

import com.fasterxml.jackson.annotation.*;

public class ExpressionOntologyTerm {
	
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;

	@JsonProperty("expression_ontology_term_id")
	private String term_id;
	
	@JsonProperty("expression_ontology_term_name")
	private String term_name;
	
	@JsonProperty("expression_ontology_term_definition")
	private String term_def;
	
	public String getTermId() {
        return term_id;
    }
    public void setTermId(String term_id) {
        this.term_id = term_id;
    }
    public String getTermName() {
        return term_name;
    }
    public void setTermName(String term_name) {
        this.term_name = term_name;
    }
    public String getTermdef() {
        return term_def;
    }
    public void setTermdef(String term_def) {
        this.term_def = term_def;
    }

}
