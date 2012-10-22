/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * Thrown to indicate that an index of a {@link NucleotideSequence} is out of
 * range.
 * 
 * @author Matthew Titmus
 */
public class SequenceIndexOutOfBoundsException extends IndexOutOfBoundsException {
    /**
     * Constructs a <code>SequenceIndexOutOfBoundsException</code> with no
     * detail message.
     */
    public SequenceIndexOutOfBoundsException() {}

    /**
     * Constructs a <code>SequenceIndexOutOfBoundsException</code> with the
     * specified detail message.
     * 
     * @param message The detail message.
     */
    public SequenceIndexOutOfBoundsException(String message) {
        super(message);
    }
}
