/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function_computation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.FloatArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Machine;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

public class Aggregation {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    /**
     * @param args
     */
	@SuppressWarnings({"deprecation"})
	public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {
	    
	    Options options = new Options();
        
        Option forceOption = new Option("f", "force", false, "force the computation of the aggregate functions "
                + "even if files already exist");
        forceOption.setRequired(false);
        options.addOption(forceOption);
        
        Option gOption = new Option("g", "group", true, "set group of datasets for which the aggregate functions"
                + " will be computed, followed by their temporal and spatial attribute indices");
        gOption.setRequired(true);
        gOption.setArgName("GROUP");
        gOption.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(gOption);
        
        Option machineOption = new Option("m", "machine", true, "machine identifier");
        machineOption.setRequired(true);
        machineOption.setArgName("MACHINE");
        machineOption.setArgs(1);
        options.addOption(machineOption);
        
        Option nodesOption = new Option("n", "nodes", true, "number of nodes");
        nodesOption.setRequired(true);
        nodesOption.setArgName("NODES");
        nodesOption.setArgs(1);
        options.addOption(nodesOption);
        
        Option s3Option = new Option("s3", "s3", false, "data on Amazon S3");
        s3Option.setRequired(false);
        options.addOption(s3Option);
        
        Option awsAccessKeyIdOption = new Option("aws_id", "aws-id", true, "aws access key id; "
                + "this is required if the execution is on aws");
        awsAccessKeyIdOption.setRequired(false);
        awsAccessKeyIdOption.setArgName("AWS-ACCESS-KEY-ID");
        awsAccessKeyIdOption.setArgs(1);
        options.addOption(awsAccessKeyIdOption);
        
        Option awsSecretAccessKeyOption = new Option("aws_key", "aws-id", true, "aws secrect access key; "
                + "this is required if the execution is on aws");
        awsSecretAccessKeyOption.setRequired(false);
        awsSecretAccessKeyOption.setArgName("AWS-SECRET-ACCESS-KEY");
        awsSecretAccessKeyOption.setArgs(1);
        options.addOption(awsSecretAccessKeyOption);
        
        Option bucketOption = new Option("b", "s3-bucket", true, "bucket on s3; "
                + "this is required if the execution is on aws");
        bucketOption.setRequired(false);
        bucketOption.setArgName("S3-BUCKET");
        bucketOption.setArgs(1);
        options.addOption(bucketOption);
        
        Option helpOption = new Option("h", "help", false, "display this message");
        helpOption.setRequired(false);
        options.addOption(helpOption);
        
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.scalar_function_computation.Aggregation", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.scalar_function_computation.Aggregation", options, true);
            System.exit(0);
        }
        
        boolean s3 = cmd.hasOption("s3");
        String s3bucket = "";
        String awsAccessKeyId = "";
        String awsSecretAccessKey = "";
        
        if (s3) {
            if ((!cmd.hasOption("aws_id")) || (!cmd.hasOption("aws_key")) ||
                    (!cmd.hasOption("b"))) {
                System.out.println("Arguments 'aws_id', 'aws_key', and 'b'"
                        + " are mandatory if execution is on AWS.");
                formatter.printHelp("hadoop jar data-polygamy.jar " +
                        "edu.nyu.vida.data_polygamy.scalar_function_computation.Aggregation", options, true);
                System.exit(0);
            }
            s3bucket = cmd.getOptionValue("b");
            awsAccessKeyId = cmd.getOptionValue("aws_id");
            awsSecretAccessKey = cmd.getOptionValue("aws_key");
        }
        
        boolean snappyCompression = false;
        boolean bzip2Compression = false;
        String machine = cmd.getOptionValue("m");
        int nbNodes = Integer.parseInt(cmd.getOptionValue("n"));
        
        Configuration s3conf = new Configuration();
        if (s3) {
            s3conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
            s3conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
            s3conf.set("bucket", s3bucket);
        }
    	
        String datasetNames = "";
        String datasetIds = "";
        String preProcessingDatasets = "";
        
    	ArrayList<String> shortDataset = new ArrayList<String>();
    	ArrayList<String> shortDatasetAggregation = new ArrayList<String>();
    	HashMap<String,String> datasetTempAtt = new HashMap<String,String>();
    	HashMap<String,String> datasetSpatialAtt = new HashMap<String,String>();
    	HashMap<String,ArrayList<String>> preProcessingDataset =
    	        new HashMap<String,ArrayList<String>>();
    	HashMap<String,String> datasetId = new HashMap<String,String>();
    	
    	boolean removeExistingFiles = cmd.hasOption("f");
    	String[] datasetArgs = cmd.getOptionValues("g");
    	
    	for (int i = 0; i < datasetArgs.length; i += 3) {
    	    String dataset = datasetArgs[i];
            
            // getting pre-processing
            ArrayList<String> tempPreProcessing =
                    FrameworkUtils.searchAllPreProcessing(dataset, s3conf, s3);
            if (tempPreProcessing == null) {
                System.out.println("No pre-processing available for " + dataset);
                continue;
            }
            preProcessingDataset.put(dataset, tempPreProcessing);
            
            shortDataset.add(dataset);
            datasetTempAtt.put(dataset, ((datasetArgs[i+1] == "null") ? null : datasetArgs[i+1]));
            datasetSpatialAtt.put(dataset, ((datasetArgs[i+2] == "null") ? null : datasetArgs[i+2]));
            
            datasetId.put(dataset, null);
    	}
    	
    	if (shortDataset.size() == 0) {
            System.out.println("No datasets to process.");
            System.exit(0);
        }
        
        // getting dataset id
        
        Path path = null;
        FileSystem fs = null;
        
        if (s3) {
            path = new Path(s3bucket + FrameworkUtils.datasetsIndexDir);
            fs = FileSystem.get(path.toUri(), s3conf);
        } else {
            fs = FileSystem.get(new Configuration());
            path = new Path (fs.getHomeDirectory() + "/" + FrameworkUtils.datasetsIndexDir);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null) {
            String[] dt = line.split("\t");
            if (datasetId.containsKey(dt[0])) {
                datasetId.put(dt[0], dt[1]);
                datasetNames += dt[0] + ",";
                datasetIds += dt[1] + ",";
            }
            line = br.readLine();
        }
        br.close();
        if (s3)
            fs.close();
        
        datasetNames = datasetNames.substring(0, datasetNames.length()-1);
        datasetIds = datasetIds.substring(0, datasetIds.length()-1);
        Iterator<String> it = shortDataset.iterator();
        while (it.hasNext()) {
            String dataset = it.next();
            if (datasetId.get(dataset) == null) {
                System.out.println("No dataset id for " + dataset);
                System.exit(0);
            }
        }
        
        FrameworkUtils.createDir(s3bucket + FrameworkUtils.aggregatesDir, s3conf, s3);
        
        HashSet<String> input = new HashSet<String>();
        
        for (String dataset: shortDataset) {
            
            for (String preProcessingFile : preProcessingDataset.get(dataset)) {
        
                boolean dataAdded = false;
                
                String aggregatesOutputFileName = s3bucket + FrameworkUtils.aggregatesDir + "/" + dataset + "/";
                
                if (removeExistingFiles) {
                    FrameworkUtils.removeFile(
                            aggregatesOutputFileName, s3conf, s3);
                }
                
                if (!FrameworkUtils.fileExists
                        (aggregatesOutputFileName, s3conf, s3)) {
                    
                    dataAdded = true;
                }
                
                if (dataAdded) {
                    input.add(s3bucket + FrameworkUtils.preProcessingDir + "/" + preProcessingFile);
                    shortDatasetAggregation.add(dataset);
                }
            }
        }
        
        if (input.isEmpty()) {
            System.out.println("All the input datasets have aggregates.");
            System.out.println("Use -f in the beginning of the command line to force the computation.");
            System.exit(0);
        }
        
        it = input.iterator();
        while (it.hasNext()) {
            preProcessingDatasets += it.next() + ",";
        }
        
    	Job aggJob = null;
    	String aggregatesOutputDir = s3bucket + FrameworkUtils.aggregatesDir + "/tmp/";
    	String jobName = "aggregates";
    	
    	FrameworkUtils.removeFile(aggregatesOutputDir, s3conf, s3);
        
        Configuration aggConf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        
        aggConf.set("dataset-name", datasetNames);
        aggConf.set("dataset-id", datasetIds);
        
        for (int i = 0; i < shortDatasetAggregation.size(); i++) {
            String dataset = shortDatasetAggregation.get(i);
            String id = datasetId.get(dataset);
            aggConf.set("dataset-" + id + "-temporal-att",
                    datasetTempAtt.get(dataset));
            aggConf.set("dataset-" + id + "-spatial-att",
                    datasetSpatialAtt.get(dataset));
            
            if (s3)
                aggConf.set("dataset-" + id,
                        s3bucket + FrameworkUtils.preProcessingDir + "/" + preProcessingDataset.get(dataset));
            else
                aggConf.set("dataset-" + id,
                        FileSystem.get(new Configuration()).getHomeDirectory() +
                        "/" + FrameworkUtils.preProcessingDir + "/" + preProcessingDataset.get(dataset));
        }
        
        aggConf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        aggConf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        aggConf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        aggConf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        aggConf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        aggConf.set("mapreduce.task.io.sort.mb", "200");
        aggConf.set("mapreduce.task.io.sort.factor", "100");
        machineConf.setMachineConfiguration(aggConf);

        aggConf.set("mapreduce.reduce.memory.mb", "10000");
        aggConf.set("mapreduce.reduce.java.opts", "-Xmx9000m");
        aggConf.set("mapreduce.task.timeout", "9000000");
        
        if (s3) {
            machineConf.setMachineConfiguration(aggConf);
            aggConf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
            aggConf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
        }
        
        if (snappyCompression) {
            aggConf.set("mapreduce.map.output.compress", "true");
            aggConf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            //aggConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        }
        if (bzip2Compression) {
            aggConf.set("mapreduce.map.output.compress", "true");
            aggConf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
            //aggConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        }
    
        aggJob = new Job(aggConf);
        aggJob.setJobName(jobName);
        
        aggJob.setMapOutputKeyClass(SpatioTemporalWritable.class);
        aggJob.setMapOutputValueClass(AggregationArrayWritable.class);
        aggJob.setOutputKeyClass(SpatioTemporalWritable.class);
        aggJob.setOutputValueClass(FloatArrayWritable.class);
        //aggJob.setOutputKeyClass(Text.class);
        //aggJob.setOutputValueClass(Text.class);
   
        aggJob.setMapperClass(AggregationMapper.class);
        aggJob.setCombinerClass(AggregationCombiner.class);
        aggJob.setReducerClass(AggregationReducer.class);
        aggJob.setNumReduceTasks(machineConf.getNumberReduces());
   
        aggJob.setInputFormatClass(SequenceFileInputFormat.class);
        //aggJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(aggJob, SequenceFileOutputFormat.class);
        //LazyOutputFormat.setOutputFormatClass(aggJob, TextOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(aggJob, true);
        SequenceFileOutputFormat.setOutputCompressionType(aggJob, CompressionType.BLOCK);
   
        FileInputFormat.setInputDirRecursive(aggJob, true);
        FileInputFormat.setInputPaths(aggJob, preProcessingDatasets.substring(0,
                preProcessingDatasets.length()-1));
        FileOutputFormat.setOutputPath(aggJob, new Path(aggregatesOutputDir));
   
        aggJob.setJarByClass(Aggregation.class);
        
        long start = System.currentTimeMillis();
        aggJob.submit();
        aggJob.waitForCompletion(true);
        System.out.println(jobName + "\t" + (System.currentTimeMillis() - start));
        
        // moving files to right place
        for (String dataset: shortDatasetAggregation) {
            String from = s3bucket + FrameworkUtils.aggregatesDir + "/tmp/" + dataset + "/";
            String to = s3bucket + FrameworkUtils.aggregatesDir + "/" + dataset + "/";
            FrameworkUtils.renameFile(from, to, s3conf, s3);
        }
        
    }

}
