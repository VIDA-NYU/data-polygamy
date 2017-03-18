/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.feature_identification;

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
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Machine;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalFloatWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;

public class IndexCreation {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    /**
     * @param args
     */
	@SuppressWarnings({"deprecation"})
	public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {
	    
	    Options options = new Options();
        
        Option forceOption = new Option("f", "force", false, "force the computation of the index and events "
                + "even if files already exist");
        forceOption.setRequired(false);
        options.addOption(forceOption);
        
        Option thresholdOption = new Option("t", "use-custom-thresholds", false,
                "use custom thresholds for regular and rare events, defined in HDFS_HOME/" + FrameworkUtils.thresholdDir + " file");
        thresholdOption.setRequired(false);
        options.addOption(thresholdOption);
        
        Option gOption = new Option("g", "group", true, "set group of datasets for which the indices and events"
                + " will be computed");
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
                    "edu.nyu.vida.data_polygamy.feature_identification.IndexCreation", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.feature_identification.IndexCreation", options, true);
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
                        "edu.nyu.vida.data_polygamy.feature_identification.IndexCreation", options, true);
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
    	
    	ArrayList<String> shortDataset = new ArrayList<String>();
    	ArrayList<String> shortDatasetIndex = new ArrayList<String>();
    	HashMap<String,String> datasetAgg = new HashMap<String,String>();
    	HashMap<String,String> datasetId = new HashMap<String,String>();
    	HashMap<String,HashMap<Integer,Double>> datasetRegThreshold = new HashMap<String,HashMap<Integer,Double>>();
    	HashMap<String,HashMap<Integer,Double>> datasetRareThreshold = new HashMap<String,HashMap<Integer,Double>>();
        
        Path path = null;
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br;
        
        boolean removeExistingFiles = cmd.hasOption("f");
        boolean isThresholdUserDefined = cmd.hasOption("t");
        
        for (String dataset : cmd.getOptionValues("g")) {

            // getting aggregates
            String[] aggregate = FrameworkUtils.searchAggregates(dataset, s3conf, s3);
            if (aggregate.length == 0) {
                System.out.println("No aggregates found for " + dataset + ".");
                continue;
            }
            
            // getting aggregates header
            String aggregatesHeaderFileName = FrameworkUtils.searchAggregatesHeader(dataset, s3conf, s3);
            if (aggregatesHeaderFileName == null) {
                System.out.println("No aggregate header for " + dataset);
                continue;
            }
            
            String aggregatesHeader = s3bucket + FrameworkUtils.preProcessingDir + "/" + aggregatesHeaderFileName;
            
            shortDataset.add(dataset);
            datasetId.put(dataset, null);
            
            if (s3) {
                path = new Path(aggregatesHeader);
                fs = FileSystem.get(path.toUri(), s3conf);
            } else {
                path = new Path (fs.getHomeDirectory() + "/" + aggregatesHeader);
            }
            
            br = new BufferedReader(new InputStreamReader(fs.open(path)));
            datasetAgg.put(dataset, br.readLine().split("\t")[1]);
            br.close();
            if (s3)
                fs.close();
        }
        
        if (shortDataset.size() == 0) {
            System.out.println("No datasets to process.");
            System.exit(0);
        }
        
        // getting dataset id

        if (s3) {
            path = new Path(s3bucket + FrameworkUtils.datasetsIndexDir);
            fs = FileSystem.get(path.toUri(), s3conf);
        } else {
            path = new Path (fs.getHomeDirectory() + "/" + FrameworkUtils.datasetsIndexDir);
        }
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
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
        
        // getting user defined thresholds
        
        if (isThresholdUserDefined) {
            if (s3) {
                path = new Path(s3bucket + FrameworkUtils.thresholdDir);
                fs = FileSystem.get(path.toUri(), s3conf);
            } else {
                path = new Path (fs.getHomeDirectory() + "/" + FrameworkUtils.thresholdDir);
            }
            br = new BufferedReader(new InputStreamReader(fs.open(path)));
            line = br.readLine();
            while (line != null) {
                // getting dataset name
                String dataset = line.trim();
                HashMap<Integer,Double> regThresholds = new HashMap<Integer,Double>();
                HashMap<Integer,Double> rareThresholds = new HashMap<Integer,Double>();
                line = br.readLine();
                while ((line != null) && (line.split("\t").length > 1)) {
                    // getting attribute ids and thresholds
                    String[] keyVals = line.trim().split("\t");
                    int att = Integer.parseInt(keyVals[0].trim());
                    regThresholds.put(att, Double.parseDouble(keyVals[1].trim()));
                    rareThresholds.put(att, Double.parseDouble(keyVals[2].trim()));
                    line = br.readLine();
                }
                datasetRegThreshold.put(dataset, regThresholds);
                datasetRareThreshold.put(dataset, rareThresholds);
            }
            br.close();
        }
        if (s3)
            fs.close();
        
        // datasets that will use existing merge tree
        ArrayList<String> useMergeTree = new ArrayList<String>(); 
        
        // creating index for each spatio-temporal resolution
        
        FrameworkUtils.createDir(s3bucket + FrameworkUtils.indexDir, s3conf, s3);
     
        HashSet<String> input = new HashSet<String>();
        
        for (String dataset: shortDataset) {
            
            String indexCreationOutputFileName = s3bucket + FrameworkUtils.indexDir + "/" + dataset + "/";
            String mergeTreeFileName = s3bucket + FrameworkUtils.mergeTreeDir + "/" + dataset + "/";
            
            if (removeExistingFiles) {
                FrameworkUtils.removeFile(indexCreationOutputFileName, s3conf, s3);
                FrameworkUtils.removeFile(mergeTreeFileName, s3conf, s3);
                FrameworkUtils.createDir(mergeTreeFileName, s3conf, s3);
            } else if (datasetRegThreshold.containsKey(dataset)) {
                FrameworkUtils.removeFile(indexCreationOutputFileName, s3conf, s3);
                if (FrameworkUtils.fileExists(mergeTreeFileName, s3conf, s3)) {
                    useMergeTree.add(dataset);
                }
            }
            
            if (!FrameworkUtils.fileExists(indexCreationOutputFileName, s3conf, s3)) {
                input.add(s3bucket + FrameworkUtils.aggregatesDir + "/" + dataset);
                shortDatasetIndex.add(dataset);
            }
            
        }
        
        if (input.isEmpty()) {
            System.out.println("All the input datasets have indices.");
            System.out.println("Use -f in the beginning of the command line to force the computation.");
            System.exit(0);
        }
        
        String aggregateDatasets = "";
        it = input.iterator();
        while (it.hasNext()) {
            aggregateDatasets += it.next() + ",";
        }
        
    	Job icJob = null;
        Configuration icConf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        
        String jobName = "index";
        String indexOutputDir = s3bucket + FrameworkUtils.indexDir + "/tmp/";
        
        FrameworkUtils.removeFile(indexOutputDir, s3conf, s3);
        
        icConf.set("dataset-name", datasetNames);
        icConf.set("dataset-id", datasetIds);
        
        if (!useMergeTree.isEmpty()) {
            String useMergeTreeStr = "";
            for (String dt : useMergeTree) {
                useMergeTreeStr += dt + ",";
            }
            icConf.set("use-merge-tree", useMergeTreeStr.substring(0, useMergeTreeStr.length()-1));
        }
        
        for (int i = 0; i < shortDataset.size(); i++) {
            String dataset = shortDataset.get(i);
            String id = datasetId.get(dataset);
            icConf.set("dataset-" + id + "-aggregates",
                    datasetAgg.get(dataset));
            if (datasetRegThreshold.containsKey(dataset)) {
                HashMap<Integer,Double> regThresholds = datasetRegThreshold.get(dataset);
                String thresholds = "";
                for (int att : regThresholds.keySet()) {
                    thresholds += String.valueOf(att) + "-" + String.valueOf(regThresholds.get(att)) + ",";
                }
                icConf.set("regular-" + id, thresholds.substring(0, thresholds.length()-1));
            }
            
            if (datasetRareThreshold.containsKey(dataset)) {
                HashMap<Integer,Double> rareThresholds = datasetRareThreshold.get(dataset);
                String thresholds = "";
                for (int att : rareThresholds.keySet()) {
                    thresholds += String.valueOf(att) + "-" + String.valueOf(rareThresholds.get(att)) + ",";
                }
                icConf.set("rare-" + id, thresholds.substring(0, thresholds.length()-1));
            }
        }
        
        icConf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        icConf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        icConf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        icConf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        icConf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        icConf.set("mapreduce.task.io.sort.mb", "200");
        icConf.set("mapreduce.task.io.sort.factor", "100");
        //icConf.set("mapreduce.task.timeout", "1800000");
        machineConf.setMachineConfiguration(icConf);

        icConf.set("mapreduce.reduce.memory.mb", "120000");
        icConf.set("mapreduce.reduce.java.opts", "-Xmx110000m");
        icConf.set("mapreduce.task.timeout", "12000000");
        
        if (s3) {
            machineConf.setMachineConfiguration(icConf);
            icConf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
            icConf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
            icConf.set("bucket", s3bucket);
        }
        
        if (snappyCompression) {
            icConf.set("mapreduce.map.output.compress", "true");
            icConf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            //icConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        }
        if (bzip2Compression) {
            icConf.set("mapreduce.map.output.compress", "true");
            icConf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
            //icConf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        }
        
        icJob = new Job(icConf);
        icJob.setJobName(jobName);
        
        icJob.setMapOutputKeyClass(AttributeResolutionWritable.class);
        icJob.setMapOutputValueClass(SpatioTemporalFloatWritable.class);
        icJob.setOutputKeyClass(AttributeResolutionWritable.class);
        icJob.setOutputValueClass(TopologyTimeSeriesWritable.class);
        //icJob.setOutputKeyClass(Text.class);
        //icJob.setOutputValueClass(Text.class);
   
        icJob.setMapperClass(IndexCreationMapper.class);
        icJob.setReducerClass(IndexCreationReducer.class);
        icJob.setNumReduceTasks(machineConf.getNumberReduces());
   
        icJob.setInputFormatClass(SequenceFileInputFormat.class);
        //icJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(icJob, SequenceFileOutputFormat.class);
        //LazyOutputFormat.setOutputFormatClass(icJob, TextOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(icJob, true);
        SequenceFileOutputFormat.setOutputCompressionType(icJob, CompressionType.BLOCK);
   
        FileInputFormat.setInputDirRecursive(icJob, true);
        FileInputFormat.setInputPaths(icJob, aggregateDatasets.substring(0,
                aggregateDatasets.length()-1));
        FileOutputFormat.setOutputPath(icJob, new Path(indexOutputDir));
   
        icJob.setJarByClass(IndexCreation.class);
        
        long start = System.currentTimeMillis();
        icJob.submit();
        icJob.waitForCompletion(true);
        System.out.println(jobName + "\t" + (System.currentTimeMillis() - start));
        
        // moving files to right place
        for (String dataset: shortDatasetIndex) {
            String from = s3bucket + FrameworkUtils.indexDir + "/tmp/" + dataset + "/";
            String to = s3bucket + FrameworkUtils.indexDir + "/" + dataset + "/";
            FrameworkUtils.renameFile(from, to, s3conf, s3);
        }
        
    }

}
