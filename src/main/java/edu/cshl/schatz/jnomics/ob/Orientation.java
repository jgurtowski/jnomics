/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

public enum Orientation {
    MINUS("-"),
    PLUS("+"),
    UNSPECIFIED("?"), ;

    private String str;

    private Orientation(String str) {
        this.str = str;
    }

    /**
     * Simply returns the opposite orientation.
     */
    public Orientation invert() {
        return (this == PLUS ? MINUS : this == MINUS ? PLUS : UNSPECIFIED);
    }

    /*
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return str;
    }
}
