package edu.cshl.schatz.jnomics.tools;


import edu.cshl.schatz.jnomics.cli.ExtendedGnuParser;
import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJobBuilder;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsMapper;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsReducer;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JnomicsMain extends Configured implements Tool {

    public static final Map<String, Class<? extends JnomicsMapper>> mapperClasses =
            new HashMap<String, Class<? extends JnomicsMapper>>() {
                {
                    put("bowtie2_map", Bowtie2Map.class);
                    put("bwa_map", BWAMap.class);
                    put("samtools_map", SamtoolsMap.class);
                    put("kcounter_map", KCounterMap.class);
                    put("templatehist_map", TemplateHistMap.class);
                    put("peloader_map", PELoaderMap.class);
                    put("readcount_map", CountReadsMap.class);
                    put("basecount_map", CountBasesMap.class);
                    put("alignmentsort_map", AlignmentSortMap.class);
                    put("cufflinks_map",CufflinksMap.class);
                    put("seloader_map",SELoaderMap.class);
                    put("read_filter_map",AlignmentFilterMap.class);
                    put("reverse_complement_map", ReverseComplementMap.class);
                    put("kcounterhist_map", KCounterHistMap.class);
                    put("readkmerdist_map", ReadKmerDistMap.class);
                    put("httploader_map", HttpLoaderMap.class);
                    put("readfilesplit_map", ReadFileSplitMap.class);
                    put("readkmeranalysis_map", ReadKmerAnalysisMap.class);
                    put("readkmerfilter_map", ReadKmerFilterMap.class);
                    put("textkmercount_map", TextKmerCountMap.class);
                    put("textkmercounthist_map",TextKmerCountHistMap.class);
                    put("textcountreadcorrected_map", TextCountReadCorrectedMap.class);
                    put("customreadkmerfilter_map", CustomReadKmerFilterMap.class);
                    put("blast_map", BlastMap.class);
                    put("pb_correct_map",PBCorrectMap.class);
                }
            };

    public static final Map<String, Class<? extends JnomicsReducer>> reducerClasses =
            new HashMap<String, Class<? extends JnomicsReducer>>() {
                {
                    put("samtools_reduce", SamtoolsReduce.class);
                    put("kcounter_reduce", KCounterReduce.class);
                    put("templatehist_reduce", TemplateHistReduce.class);
                    put("peloader_reduce",PELoaderReduce.class);
                    put("countlong_reduce",CountLongReduce.class);
                    put("alignmentsort_reduce", AlignmentSortReduce.class);
                    put("cufflinks_reduce",CufflinksReduce.class);
                    put("seloader_reduce",SELoaderReduce.class);
                    //put("gatk_realign_reduce", GATKRealignReduce.class);
                    put("gatk_unified_genotyper_reduce", GATKUnifiedGenotyper.class);
                    put("gatk_haplotypecaller_reduce", GATKHaplotypeCaller.class);
                    put("kcounterhist_reduce",KCounterHistReduce.class);
                    //put("httploader_reduce", HttpLoaderReduce.class);
                    put("readfilesplit_reduce", ReadFileSplitReduce.class);
                    put("textkmercount_reduce", TextKmerCountReduce.class);
                    put("textkmercounthist_reduce",TextKmerCountHistReduce.class);
                    put("textcountreadcorrected_reduce",TextCountReadCorrectedReduce.class);
                    put("coverage_reduce", CoverageReduce.class);
                    put("pb_correct_reduce",PBCorrectReduce.class);
                }
            };

    public static final Map<String, Class> helperClasses =
            new HashMap<String, Class>() {
                {
                    //put("loader_pairend", PairedEndLoader.class);
                }
            };


    public static void printMainMenu() {
        System.out.println("Options:");
        System.out.println("");
        System.out.println("mapper-list\t:\tList available mappers");
        System.out.println("reducer-list\t:\tList available reducers");
        System.out.println("describe\t:\tDescribe a mapper or reducer");
        //System.out.println("helper-task-list\t:\tList all helper tasks");
        //System.out.println("helper-task\t:\tRun helper task");
        System.out.println("loader-pe\t:\tLoad paired end sequencing file into hdfs");
        System.out.println("loader-se\t:\tLoad single end sequencing file into hdfs");
        System.out.println("fasta-loader\t:\t Load fasta file");
        System.out.println("hdfs-stream\t:\tStream data to hdfs");                     
        System.out.println("alignment-extract\t:\textract alignments");
        System.out.println("manifest-loader\t:\tLoad manifest file into hdfs");
        System.out.println("vcf-merge\t:\tMerge vcf files");
        System.out.println("covariate-merge\t:\tMerge GATK Covariate files");
        System.out.println("job\t:\tsubmit a job");
    }

    public static void main(String[] args) throws Exception {

        //hack for snappy codec in fat jar
        //String jar_path = JnomicsMain.class.getProtectionDomain().getCodeSource().getLocation().toString();
        //System.setProperty("org.xerial.snappy.lib.path", "/tmp");
        //System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.so");

        //for(String n :System.getProperties().stringPropertyNames()){
        //System.out.println(n +" : " +System.getProperty(n));
        //}


        if (args.length < 1) {
            printMainMenu();
            System.exit(-1);
        }

        if (args[0].compareTo("mapper-list") == 0) {
            System.out.println("Available Mappers:");
            for (Object t : mapperClasses.keySet()) {
                System.out.println(t);
            }
        } else if (args[0].compareTo("reducer-list") == 0) {
            System.out.println("Available Reducers:");
            for (Object t : reducerClasses.keySet()) {
                System.out.println(t);
            }
        } else if (args[0].compareTo("loader-pe") == 0) {
            PairedEndLoader.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].compareTo("loader-se") == 0){
            SingleEndLoader.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].compareTo("fasta-loader") ==0){
            FastaLoader.main(Arrays.copyOfRange(args,1,args.length));
        }else if (args[0].compareTo("hdfs-stream") ==0){
            StreamFileToHDFS.main(Arrays.copyOfRange(args,1,args.length));
        } else if (args[0].compareTo("alignment-extract") ==0){
            AlignmentSortExtract.main(Arrays.copyOfRange(args, 1, args.length));
        }else if (args[0].compareTo("manifest-loader") == 0){
            //ManifestLoader.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].compareTo("vcf-merge") == 0){
            VCFMerge.main(Arrays.copyOfRange(args,1,args.length));
        }else if(args[0].compareTo("covariate-merge") == 0){
            CovariateMerge.main(Arrays.copyOfRange(args,1,args.length));
        }else if (args[0].compareTo("helper-task-list") == 0) {
            System.out.println("Available Helper Tasks:");
            for (Object t : helperClasses.keySet()) {
                System.out.println(t);
            }
        } else if (args[0].compareTo("helper-task") == 0) {
            if (args.length < 2) {
                System.out.println("run helper-task-list to see available helper tasks");
            } else {
                Class exec = helperClasses.get(args[1]);
                if (exec == null) {
                    System.out.println("Error: unknown helper " + args[1]);
                } else {
                    System.out.println("TODO: Implement runner");
                }
            }
        } else if (args[0].compareTo("describe") == 0) {
            boolean found = false;
            if (args.length < 2) {
                System.out.println("describe <mapper/reducer>");
            }
            for (String t : mapperClasses.keySet()) {
                if (args[1].compareTo(t) == 0) {
                    JnomicsArgument.printUsage(args[1] + " Arguments:", mapperClasses.get(t).newInstance().getArgs(),
                            System.out);
                    found = true;
                }
            }
            for (String t : reducerClasses.keySet()) {
                if (args[1].compareTo(t) == 0) {
                    JnomicsArgument.printUsage(args[1] + " Arguments:", reducerClasses.get(t).newInstance().getArgs(),
                            System.out);
                    found = true;
                }
            }
            if (!found) {
                System.out.println("Unknwon mapper/reducer" + args[1]);
            }

        } else if (args[0].compareTo("job") == 0) {
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, args.length - 1);
            System.exit(ToolRunner.run(new Configuration(), new JnomicsMain(), newArgs));
        } else {
            printMainMenu();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        /** Standard options **/
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("required").withLongOpt("mapper").isRequired(true).hasArg().create());
        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("reducer").isRequired(false).hasArg().create());
        options.addOption(OptionBuilder.withArgName("required").withLongOpt("in").isRequired(true).hasArg().create());
        options.addOption(OptionBuilder.withArgName("required").withLongOpt("out").isRequired(true).hasArg().create());
        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("archives").isRequired(false).hasArg().create());
        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("num_reducers").isRequired(false).hasArg().create());
        options.addOption(OptionBuilder.withArgName("optional").withLongOpt("max_split_size").isRequired(false).hasArg().create());
        
        ExtendedGnuParser parser = new ExtendedGnuParser(true);
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cli = null;
        try{
            cli = parser.parse(options,args,false);
        }catch(ParseException e){
            formatter.printHelp(e.toString(),options);
            return -1;
        }
        
        JnomicsJobBuilder builder = null;

        /** Set Mapper Class **/
        Class<? extends JnomicsMapper> mapperClass;
        if(mapperClasses.containsKey(cli.getOptionValue("mapper"))){
            mapperClass = mapperClasses.get(cli.getOptionValue("mapper"));
            builder = new JnomicsJobBuilder(getConf(),mapperClass);
        }else{
            formatter.printHelp("bad mapper: "+ cli.getOptionValue("mapper"),options);
            return -1;
        }

        /** Set Reducer class **/
        if(cli.hasOption("reducer")){
            if(reducerClasses.containsKey(cli.getOptionValue("reducer"))){
                builder.setReducerClass(reducerClasses.get(cli.getOptionValue("reducer")));
            }else{
                formatter.printHelp("bad reducer: " + cli.getOptionValue("reducer"),options);
                return -1;
            }
        }

        /** Set input and output path**/
        builder.setInputPath(cli.getOptionValue("in"))
                .setOutputPath(cli.getOptionValue("out"));

        /**Add any archives to distributed cache, requires full uri (hdfs://namenode:port/...)**/
        if(cli.hasOption("archives")){
            String []archives = cli.getOptionValue("archives").split(",");
            for(String archive : archives){
                builder.addArchive(archive);
            }
        }

        /**Set num map tasks**/
        if(cli.hasOption("num_mappers")){
            builder.setMapTasks(Integer.parseInt(cli.getOptionValue("num_mappers")));
        }

        /**Set num reduce tasks**/
        if(cli.hasOption("num_reducers")){
            builder.setReduceTasks(Integer.parseInt(cli.getOptionValue("num_reducers")));
        }
        
        if(cli.hasOption("max_split_size")){
            builder.setMaxSplitSize(Integer.parseInt(cli.getOptionValue("max_split_size")));
        }
        
        
        /**get additional args from mapper and reducer selected**/
        for( JnomicsArgument arg: builder.getArgs() ){
            options.addOption(OptionBuilder.withArgName(arg.isRequired() ? "required" : "optional")
                    .withLongOpt(arg.getName()).isRequired(arg.isRequired()).hasArg().create());
        }

        /**reparse the arguments with the additonal options**/
        try{
            cli = parser.parse(options,args);
        }catch(ParseException e){
            formatter.printHelp(e.toString(),options);
            return -1;
        }

        for(JnomicsArgument arg: builder.getArgs()){
            if(cli.hasOption(arg.getName())){
                builder.setParam(arg.getName(),cli.getOptionValue(arg.getName()));
            }
        }

        builder.setJobName(mapperClass.getSimpleName()+"-"+cli.getOptionValue("in"));
        
        Job job = new Job(builder.getJobConf());
        job.setJarByClass(JnomicsMain.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
