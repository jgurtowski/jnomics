/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.ob;

/**
 * Thrown by the <code>break()</code> method of the {@link SequenceSegment}
 * class.
 * 
 * @author Matthew Titmus
 */
public class OutOfRangeException extends RuntimeException {

    private static final long serialVersionUID = 1015338010745175305L;

    /**
	 * 
	 */
    public OutOfRangeException() {}

    /**
     * @param message
     */
    public OutOfRangeException(String message) {
        super(message);
    }

    /**
     * @param message
     * @param cause
     */
    public OutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param cause
     */
    public OutOfRangeException(Throwable cause) {
        super(cause);
    }
}
