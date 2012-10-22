package edu.cshl.schatz.jnomics.cli;


import org.apache.commons.cli.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

public class JnomicsArgument {


    private static final Logger logger = LoggerFactory.getLogger(JnomicsArgument.class);

    private String name;
    private String description;
    private String value;
    private boolean required;
    private boolean valueSet;

    public JnomicsArgument(String name, String description, boolean required){
        this.name = name;
        this.description = description;
        this.required = required;
        this.value = null;
    }
    
    public String getName(){
        return name;
    }
    
    public boolean isRequired(){
        return required;
    }
    
    public String getDescription(){
        return description;
    }

    public void setValue(String value){
        this.value = value;
        valueSet = true;
    }

    public boolean isValueSet(){
        return valueSet;
    }
    
    public String getValue(){
        return value;
    }
    
    public static int parse(JnomicsArgument[] jargs, String[] cliargs) throws ParseException {

        CommandLineParser parser = new TolerantParser();
        Options options = new Options();
        for(JnomicsArgument jarg : jargs){
            Option n = new Option(jarg.getName(),true, jarg.getDescription());
            n.setRequired(jarg.isRequired());
            options.addOption(n);
        }
        CommandLine commandline = parser.parse(options, cliargs, false);

        /*
        for (String unrecognizedOption : commandline.getArgs()) {
            if (unrecognizedOption.startsWith("-")) {
                throw new ParseException("Unrecognized option " + unrecognizedOption);
            }
        } */

        for (Option o : commandline.getOptions()) {
            String opt = o.getOpt();
            boolean handled = false;
            for(JnomicsArgument jarg: jargs){
                if(opt.equals(jarg.getName())){
                    jarg.setValue(o.getValue());
                    handled = true;
                    break;
                }
            }
            if(!handled)
                logger.warn("Unhandled parameter: " + opt + " " + o.getValue());
        }

        return 1;
    }

    public static void printUsage(String title, JnomicsArgument []args, PrintStream out){

        out.println(title);
        for(JnomicsArgument arg: args){
            String req = arg.isRequired() ? "Required" : "Optional";
            out.println("-"+arg.getName()+"\t:\t"+arg.getDescription()+ "\t:\t"+ req);
        }
    }
}
