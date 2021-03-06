/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.cli;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;

/**
 * Extends the {@link Option} class to include a "weight" value, which
 * determines its position in the options list produced by the printHelp()
 * method.
 */
public class WeightedOption extends Option {
    public static final int DEFAULT_WEIGHT = 0;

    private static final long serialVersionUID = -6671759154295486127L;

    private int weight = DEFAULT_WEIGHT;

    /**
     * Creates an Option with no properties set.
     */
    public WeightedOption() throws IllegalArgumentException {
        super(null, null);
    }

    /**
     * Creates an Option using the specified parameters.
     * 
     * @param opt short representation of the option
     * @throws IllegalArgumentException if there are any non valid Option
     *             characters in <code>opt</code>.
     */
    public WeightedOption(char opt) throws IllegalArgumentException {
        super(String.valueOf(opt), null);
    }

    /**
     * Creates an Option using the specified parameters.
     * 
     * @param opt short representation of the option
     * @throws IllegalArgumentException if there are any non valid Option
     *             characters in <code>opt</code>.
     */
    public WeightedOption(String opt) throws IllegalArgumentException {
        super(opt, null);
    }

    /**
     * Creates an Option using the specified parameters.
     * 
     * @param opt short representation of the option
     * @param hasArg specifies whether the Option takes an argument or not
     * @param description describes the function of the option
     * @throws IllegalArgumentException if there are any non valid Option
     *             characters in <code>opt</code>.
     */
    public WeightedOption(String opt, boolean hasArg, String description)
            throws IllegalArgumentException {

        super(opt, hasArg, description);
    }

    /**
     * Creates an Option using the specified parameters.
     * 
     * @param opt short representation of the option
     * @param description describes the function of the option
     * @throws IllegalArgumentException if there are any non valid Option
     *             characters in <code>opt</code>.
     */
    public WeightedOption(String opt, String description) throws IllegalArgumentException {
        super(opt, description);
    }

    /**
     * Creates an Option using the specified parameters.
     * 
     * @param opt short representation of the option
     * @param longOpt the long representation of the option
     * @param hasArg specifies whether the Option takes an argument or not
     * @param description describes the function of the option
     * @throws IllegalArgumentException if there are any non valid Option
     *             characters in <code>opt</code>.
     */
    public WeightedOption(String opt, String longOpt, boolean hasArg, String description)
            throws IllegalArgumentException {

        super(opt, longOpt, hasArg, description);
    }

    /**
     * A weight value used to determine the order of this option in the "help"
     * list generated by a {@link HelpFormatter} using a
     * {@link OptionWeightComparater} to sort. Lower values result in a position
     * nearer the bottom of the list. Any integer value is legal; the default
     * value is 0.
     */
    public int getWeight() {
        return weight;
    }

    /**
     * A weight value used to determine the order of this option in the "help"
     * list generated by a {@link HelpFormatter} using a
     * {@link OptionWeightComparater} to sort. Lower values result in a position
     * nearer the bottom of the list. Any integer value is legal; the default
     * value is 0.
     */
    public void setWeight(int weight) {
        this.weight = weight;
    }
}