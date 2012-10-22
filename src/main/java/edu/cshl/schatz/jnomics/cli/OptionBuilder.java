/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.cli;

import org.apache.commons.cli.Option;

/**
 * Modified version of the OptionBuilder from Apache Commons
 * (org.apache.commons.cli).
 * <p>
 * OptionBuilder allows the user to create Options using descriptive methods.
 * <p>
 * Details on the Builder pattern can be found at <a
 * href="http://c2.com/cgi-bin/wiki?BuilderPattern">
 * http://c2.com/cgi-bin/wiki?BuilderPattern</a>.
 * </p>
 * 
 * @author John Keyes (john at integralsource.com)
 * @version $Revision: 754830 $, $Date: 2009-03-16 00:26:44 -0700 (Mon, 16 Mar
 *          2009) $
 * @since 1.0
 */
public final class OptionBuilder {
    /** argument name */
    private String argName;

    /** option description */
    private String description;

    /** long option */
    private String longopt;

    /** the number of arguments */
    private int numberOfArgs = Option.UNINITIALIZED;

    /** option can have an optional argument value */
    private boolean optionalArg;

    /** is required? */
    private boolean required;

    /** option type */
    private Object type;

    /** value separator for argument value */
    private char valuesep;

    /** weight value */
    private int weight = WeightedOption.DEFAULT_WEIGHT;

    /**
     * Create an Option using the current settings
     * 
     * @return the Option instance
     * @throws IllegalArgumentException if <code>longOpt</code> has not been
     *             set.
     */
    public Option create() throws IllegalArgumentException {
        if (longopt == null) {
            reset();
            throw new IllegalArgumentException("must specify longopt");
        }

        return create(null);
    }

    /**
     * Create an Option using the current settings and with the specified Option
     * <code>char</code>.
     * 
     * @param opt the character representation of the Option
     * @return the Option instance
     * @throws IllegalArgumentException if <code>opt</code> is not a valid
     *             character. See Option.
     */
    public Option create(char opt) throws IllegalArgumentException {
        return create(String.valueOf(opt));
    }

    /**
     * Create an Option using the current settings and with the specified Option
     * <code>char</code>.
     * 
     * @param opt the <code>java.lang.String</code> representation of the Option
     * @return the Option instance
     * @throws IllegalArgumentException if <code>opt</code> is not a valid
     *             character. See Option.
     */
    public Option create(String opt) throws IllegalArgumentException {
        WeightedOption option = null;

        try {
            // create the option
            option = new WeightedOption(opt, description);

            // set the option properties
            option.setLongOpt(longopt);
            option.setRequired(required);
            option.setOptionalArg(optionalArg);
            option.setArgs(numberOfArgs);
            option.setType(type);
            option.setValueSeparator(valuesep);
            option.setArgName(argName);
            option.setWeight(weight);
        } finally {
            // reset the OptionBuilder properties
            reset();
        }

        // return the Option instance
        return option;
    }

    /**
     * The next Option created will require an argument value.
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasArg() {
        numberOfArgs = 1;

        return this;
    }

    /**
     * The next Option created will require an argument value if
     * <code>hasArg</code> is true.
     * 
     * @param hasArg if true then the Option has an argument value
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasArg(boolean hasArg) {
        numberOfArgs = hasArg ? 1 : Option.UNINITIALIZED;

        return this;
    }

    /**
     * The next Option created can have unlimited argument values.
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasArgs() {
        numberOfArgs = Option.UNLIMITED_VALUES;

        return this;
    }

    /**
     * The next Option created can have <code>num</code> argument values.
     * 
     * @param num the number of args that the option can have
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasArgs(int num) {
        numberOfArgs = num;

        return this;
    }

    /**
     * The next Option can have an optional argument.
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasOptionalArg() {
        numberOfArgs = 1;
        optionalArg = true;

        return this;
    }

    /**
     * The next Option can have an unlimited number of optional arguments.
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasOptionalArgs() {
        numberOfArgs = Option.UNLIMITED_VALUES;
        optionalArg = true;

        return this;
    }

    /**
     * The next Option can have the specified number of optional arguments.
     * 
     * @param numArgs - the maximum number of optional arguments the next Option
     *            created can have.
     * @return the OptionBuilder instance
     */
    public OptionBuilder hasOptionalArgs(int numArgs) {
        numberOfArgs = numArgs;
        optionalArg = true;

        return this;
    }

    /**
     * The next Option created will be required.
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder isRequired() {
        required = true;

        return this;
    }

    /**
     * The next Option created will be required if <code>required</code> is
     * true.
     * 
     * @param newRequired if true then the Option is required
     * @return the OptionBuilder instance
     */
    public OptionBuilder isRequired(boolean newRequired) {
        required = newRequired;

        return this;
    }

    /**
     * The next Option created will have the specified argument value name.
     * 
     * @param name the name for the argument value
     * @return the OptionBuilder instance
     */
    public OptionBuilder withArgName(String name) {
        argName = name;

        return this;
    }

    /**
     * The next Option created will have the specified description
     * 
     * @param newDescription a description of the Option's purpose
     * @return the OptionBuilder instance
     */
    public OptionBuilder withDescription(String newDescription) {
        description = newDescription;

        return this;
    }

    /**
     * The next Option created will have the following long option value.
     * 
     * @param newLongopt the long option value
     * @return the OptionBuilder instance
     */
    public OptionBuilder withLongOpt(String newLongopt) {
        longopt = newLongopt;

        return this;
    }

    /**
     * The next Option created will have a value that will be an instance of
     * <code>type</code>.
     * 
     * @param newType the type of the Options argument value
     * @return the OptionBuilder instance
     */
    public OptionBuilder withType(Object newType) {
        type = newType;

        return this;
    }

    /**
     * The next Option created uses '<code>=</code>' as a means to separate
     * argument values. <b>Example:</b>
     * 
     * <pre>
     * Option opt = withValueSeparator().create('D');
     * 
     * CommandLine line = parser.parse(args);
     * String propertyName = opt.getValue(0);
     * String propertyValue = opt.getValue(1);
     * </pre>
     * 
     * @return the OptionBuilder instance
     */
    public OptionBuilder withValueSeparator() {
        valuesep = '=';

        return this;
    }

    /**
     * The next Option created uses <code>sep</code> as a means to separate
     * argument values. <b>Example:</b>
     * 
     * <pre>
     * Option opt = withValueSeparator(':').create('D');
     * 
     * CommandLine line = parser.parse(args);
     * String propertyName = opt.getValue(0);
     * String propertyValue = opt.getValue(1);
     * </pre>
     * 
     * @param sep The value separator to be used for the argument values.
     * @return the OptionBuilder instance
     */
    public OptionBuilder withValueSeparator(char sep) {
        valuesep = sep;

        return this;
    }

    /**
     * The next Option created uses <code>weight</code> as its weight value for
     * the purposes for assigning position in help menus.
     */
    public OptionBuilder withWeight(int newWeight) {
        weight = newWeight;

        return this;
    }

    /**
     * Resets the member variables to their default values.
     */
    private void reset() {
        description = null;
        argName = "arg";
        longopt = null;
        type = null;
        required = false;
        numberOfArgs = Option.UNINITIALIZED;
        weight = WeightedOption.DEFAULT_WEIGHT;

        // PMM 9/6/02 - these were missing
        optionalArg = false;
        valuesep = (char) 0;
    }
}
