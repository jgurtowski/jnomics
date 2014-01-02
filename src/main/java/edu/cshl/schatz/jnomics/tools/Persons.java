package edu.cshl.schatz.jnomics.tools;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Persons {

	/**
	 * 
	 */
	//private static final long serialVersionUID = 1L;

	@JsonProperty("email")
	private String email;
	
	@JsonProperty("firstname")
	private String firstname ;
	
	@JsonProperty("lastname")
	private String lastname;
	
	@JsonProperty("institution")
	private String institution;

	
	public String getEmail() {
        return email;
    }
	public void setEmail(String email) {
	    this.email = email;
	}
	public String getFirstName() {
        return firstname;
    }
	public void setFirstName(String firstname) {
	    this.firstname = firstname;
	}
	public String getLastName() {
        return lastname;
    }
	public void setLastName(String lastname) {
	    this.lastname = lastname;
	}
	public String getInstitution() {
        return institution;
    }
	public void setInstitution(String institution) {
	    this.institution = institution;
	}
	

}
