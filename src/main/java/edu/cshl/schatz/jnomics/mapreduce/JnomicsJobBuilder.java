package edu.cshl.schatz.jnomics.mapreduce;

import edu.cshl.schatz.jnomics.cli.JnomicsArgument;
import edu.cshl.schatz.jnomics.util.TextUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: james
 */
public class JnomicsJobBuilder {

    Configuration conf;
    Class<? extends JnomicsMapper> mapper;
    Class<? extends JnomicsReducer> reducer;

    List<String> archives = new ArrayList<String>();
    
    public JnomicsJobBuilder(Configuration conf,
                      Class<? extends JnomicsMapper> mapper,
                      Class<? extends JnomicsReducer> reducer){
        this.conf = conf;
        this.mapper = mapper;
        this.reducer = reducer;
    }

    public JnomicsJobBuilder(Class<? extends JnomicsMapper> mapper, Class<? extends JnomicsReducer> reducer){
        this(new Configuration(), mapper,reducer);
    }
    
    public JnomicsJobBuilder(Class<? extends JnomicsMapper> mapper){
        this(mapper,null);
    }

    public JnomicsJobBuilder(Configuration conf, Class<? extends JnomicsMapper> mapper){
        this(conf,mapper,null);
    }

    public JnomicsJobBuilder setParam(String name, String value){
        conf.set(name,value);
        return this;
    }

    public JnomicsJobBuilder addConfModifiers(Map<String,String> modifiers){
        for(Map.Entry<String,String> entry: modifiers.entrySet()){
            conf.set(entry.getKey(),entry.getValue());
        }
        return this;
    }
    
    public JnomicsJobBuilder setJobName(String name){
        conf.set("mapred.job.name", name);
        return this;
    }

    public JnomicsJobBuilder setReducerClass(Class<? extends JnomicsReducer> reducer){
        this.reducer = reducer;
        return this;
    }
    
    public JnomicsJobBuilder addArchive(String archive){
        archives.add(archive);
        return this;
    }
    
    private void ifNotSetConf(String key, String value){
        if(null == conf.get(key,null))
            conf.set(key,value);
    }

    public JnomicsJobBuilder setReduceTasks(int num){
        conf.setInt("mapred.reduce.tasks", num);
        return this;
    }
    
    public JnomicsJobBuilder setInputPath(String in){
        conf.set("mapred.input.dir",in);
        return this;
    }
    
    public JnomicsJobBuilder setOutputPath(String out){
        conf.set("mapred.output.dir", out);
        return this;
    }

    /**Returns Arguments that are wanted by the mapper and/or reducer **/
    public JnomicsArgument[] getArgs() throws Exception{
        JnomicsMapper mapperInst = mapper.newInstance();
        JnomicsArgument[] args = mapperInst.getArgs();
        if(null != reducer){
            JnomicsReducer reducerInst = reducer.newInstance();
            args = ArrayUtils.addAll(args, reducerInst.getArgs());
        }
        return args;
    }
    
    public Configuration getJobConf() throws Exception{
        JnomicsArgument[] jArgs;
        
        //Standard items
        ifNotSetConf("mapred.used.genericoptionsparser","true");
        ifNotSetConf("mapred.mapper.new-api", "true");
        ifNotSetConf("mapred.reducer.new-api", "true");

        conf.set("mapreduce.map.class",mapper.getName());
        JnomicsMapper mapperInst = mapper.newInstance();
        conf.set("mapreduce.inputformat.class",mapperInst.getInputFormat().getName());
        conf.set("mapred.mapoutput.key.class",mapperInst.getOutputKeyClass().getName());
        conf.set("mapred.mapoutput.value.class", mapperInst.getOutputValueClass().getName());
        if(null != mapperInst.getCombinerClass()){
            conf.set("mapreduce.combine.class",mapperInst.getCombinerClass().getName());
        }
        addConfModifiers(mapperInst.getConfModifiers());
        jArgs = mapperInst.getArgs();

        if(null != reducer){
            conf.set("mapreduce.reduce.class",reducer.getName());
            JnomicsReducer reducerInst = reducer.newInstance();

            conf.set("mapreduce.outputformat.class",reducerInst.getOutputFormat().getName());
            
            addConfModifiers(reducerInst.getConfModifiers());
            jArgs = (JnomicsArgument []) ArrayUtils.addAll(jArgs,reducerInst.getArgs());
            Class grouper = reducerInst.getGrouperClass();
            if( null != grouper )
                conf.set("mapred.output.value.groupfn.class",grouper.getName());

            Class partitioner = reducerInst.getPartitionerClass();
            if( null != partitioner )
                conf.set("mapreduce.partitioner.class",partitioner.getName());

            if(null == reducerInst.getOutputKeyClass())//if we don't know what the key is, use whatever comes from the mapper
                conf.set("mapred.output.key.class",mapperInst.getOutputKeyClass().getName());
            else
                conf.set("mapred.output.key.class",reducerInst.getOutputKeyClass().getName());
            conf.set("mapred.output.value.class",reducerInst.getOutputValueClass().getName());
        }else{
            conf.set("mapred.output.key.class", conf.get("mapred.mapoutput.key.class"));
            conf.set("mapred.output.value.class", conf.get("mapred.mapoutput.value.class"));
            conf.setInt("mapred.reduce.tasks",0);

            //mapper only job, use mapper outputformat
            conf.set("mapreduce.outputformat.class", mapperInst.getOutputFormat().getName());
        }
        
        if(archives.size() > 0){
            conf.set("mapred.cache.archives",TextUtil.join(",",archives));
            conf.set("mapred.create.symlink", "yes");
            conf.set("mapred.job.cache.archives.visibilities","true");
        }

        //check params are set
        for(JnomicsArgument arg: jArgs){
            if(null == conf.get(arg.getName(),null) && arg.isRequired()){
                throw new Exception("Missing required arg: "+arg.getName());
            }
        }
        
        return conf;
    }

}
