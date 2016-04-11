/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.standard_techniques;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Machine;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.PairAttributeWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalValueWritable;

public class CorrelationTechniques {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    public static void addDatasets(String[] groupCmd, ArrayList<String> group,
            ArrayList<String> datasets, HashMap<String,String> datasetAgg,
            Path path, FileSystem fs, Configuration s3conf, boolean s3, String s3bucket) throws IOException {
        for (String dataset : groupCmd) {
            
            // getting indices
            
            String[] index = FrameworkUtils.searchAggregates(dataset, s3conf, s3);
            if (index.length == 0) {
                System.out.println("No indices found for " + dataset + ".");
                continue;
            }
            
            // getting aggregate headers
            
            String aggregatesHeaderFileName1 = FrameworkUtils.searchAggregatesHeader(dataset, s3conf, s3);
            if (aggregatesHeaderFileName1 == null) {
                System.out.println("No aggregate header for " + dataset);
                continue;
            }
            String aggregatesHeader1 = s3bucket + FrameworkUtils.preProcessingDir + "/" + aggregatesHeaderFileName1;
            
            if (s3) {
                path = new Path(aggregatesHeader1);
                fs = FileSystem.get(path.toUri(), s3conf);
            } else {
                path = new Path (fs.getHomeDirectory() + "/" + aggregatesHeader1);
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            datasetAgg.put(dataset, br.readLine().split("\t")[1]);
            br.close();
            if (s3)
                fs.close();
            
            datasets.add(dataset);
            group.add(dataset);
        }
    }
    
    /**
     * @param args
     * @throws ParseException 
     */
	@SuppressWarnings({"deprecation"})
	public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {
	    
	    Options options = new Options();
	    
	    Option forceOption = new Option("f", "force", false, "force the computation of the relationship "
	            + "even if files already exist");
	    forceOption.setRequired(false);
	    options.addOption(forceOption);
        
        Option g1Option = new Option("g1", "first-group", true, "set first group of datasets");
        g1Option.setRequired(true);
        g1Option.setArgName("FIRST GROUP");
        g1Option.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(g1Option);
        
        Option g2Option = new Option("g2", "second-group", true, "set second group of datasets");
        g2Option.setRequired(false);
        g2Option.setArgName("SECOND GROUP");
        g2Option.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(g2Option);
        
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
                        "edu.nyu.vida.data_polygamy.standard_techniques.CorrelationTechniques", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                        "edu.nyu.vida.data_polygamy.standard_techniques.CorrelationTechniques", options, true);
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
                        "edu.nyu.vida.data_polygamy.standard_techniques.CorrelationTechniques", options, true);
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
        
        Path path = null;
        FileSystem fs = FileSystem.get(new Configuration());
        
    	ArrayList<String> shortDataset = new ArrayList<String>();
    	ArrayList<String> firstGroup = new ArrayList<String>();
    	ArrayList<String> secondGroup = new ArrayList<String>();
    	HashMap<String,String> datasetAgg = new HashMap<String,String>();
    	
    	boolean removeExistingFiles = cmd.hasOption("f");
    	
    	String[] firstGroupCmd = cmd.getOptionValues("g1");
    	String[] secondGroupCmd = cmd.hasOption("g2") ? cmd.getOptionValues("g2") : new String[0];
    	addDatasets(firstGroupCmd, firstGroup, shortDataset, datasetAgg, path, fs, s3conf, s3, s3bucket);
    	addDatasets(secondGroupCmd, secondGroup, shortDataset, datasetAgg, path, fs, s3conf, s3, s3bucket);
    	
    	if (shortDataset.size() == 0) {
    	    System.out.println("No datasets to process.");
    	    System.exit(0);
    	}
    	
    	if (firstGroup.isEmpty()) {
    	    System.out.println("First group of datasets (G1) is empty. "
    	            + "Doing G1 = G2.");
    	    firstGroup.addAll(secondGroup);
    	}
    	
    	if (secondGroup.isEmpty()) {
            System.out.println("Second group of datasets (G2) is empty. "
                    + "Doing G2 = G1.");
            secondGroup.addAll(firstGroup);
        }
        
        // getting dataset ids
        
        String datasetNames = "";
        String datasetIds = "";
        HashMap<String,String> datasetId = new HashMap<String,String>();
        Iterator <String> it = shortDataset.iterator();
        while (it.hasNext()) {
            datasetId.put(it.next(), null);
        }
        
        if (s3) {
            path = new Path(s3bucket + FrameworkUtils.datasetsIndexDir);
            fs = FileSystem.get(path.toUri(), s3conf);
        } else {
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
        it = shortDataset.iterator();
        while (it.hasNext()) {
            String dataset = it.next();
            if (datasetId.get(dataset) == null) {
                System.out.println("No dataset id for " + dataset);
                System.exit(0);
            }
        }
        
        String firstGroupStr = "";
        String secondGroupStr = "";
        for (String dataset : firstGroup) {
            firstGroupStr += datasetId.get(dataset) + ",";
        }
        for (String dataset : secondGroup) {
            secondGroupStr += datasetId.get(dataset) + ",";
        }
        firstGroupStr = firstGroupStr.substring(0, firstGroupStr.length()-1);
        secondGroupStr = secondGroupStr.substring(0, secondGroupStr.length()-1);
        
        FrameworkUtils.createDir(s3bucket + FrameworkUtils.correlationTechniquesDir, s3conf, s3);
        
        String dataAttributesInputDirs = "";
        String noRelationship = "";
        
        HashSet<String> dirs = new HashSet<String>();
        
        String dataset1;
        String dataset2;
        String datasetId1;
        String datasetId2;
        for (int i = 0; i < firstGroup.size(); i++) {
            for (int j = 0; j < secondGroup.size(); j++) {
                
                if (Integer.parseInt(datasetId.get(firstGroup.get(i))) < Integer.parseInt(datasetId.get(secondGroup.get(j)))) {
                    dataset1 = firstGroup.get(i);
                    dataset2 = secondGroup.get(j);
                } else {
                    dataset1 = secondGroup.get(j);
                    dataset2 = firstGroup.get(i);
                }
                
                datasetId1 = datasetId.get(dataset1);
                datasetId2 = datasetId.get(dataset2);
                
                if (dataset1.equals(dataset2)) continue;
                String correlationOutputFileName = s3bucket + FrameworkUtils.correlationTechniquesDir + "/" + dataset1 + "-" + dataset2 + "/";
                
                if (removeExistingFiles) {
                    FrameworkUtils.removeFile(correlationOutputFileName, s3conf, s3);
                }
                if (!FrameworkUtils.fileExists(correlationOutputFileName, s3conf, s3)) {
                    dirs.add(s3bucket + FrameworkUtils.aggregatesDir + "/" + dataset1);
                    dirs.add(s3bucket + FrameworkUtils.aggregatesDir + "/" + dataset2);
                } else {
                    noRelationship += datasetId1 + "-" + datasetId2 + ","; 
                }
            }
        }
        
        if (dirs.isEmpty()) {
            System.out.println("All the relationships were already computed.");
            System.out.println("Use -f in the beginning of the command line to force the computation.");
            System.exit(0);
        }
        
        for (String dir: dirs) {
            dataAttributesInputDirs += dir + ",";
        }

        Configuration conf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        
        String jobName = "correlation";
        String correlationOutputDir = s3bucket + FrameworkUtils.correlationTechniquesDir + "/tmp/";
        
        FrameworkUtils.removeFile(correlationOutputDir, s3conf, s3);
        
        for (int i = 0; i < shortDataset.size(); i++) {
            conf.set("dataset-" + datasetId.get(shortDataset.get(i)) + "-agg",
                    datasetAgg.get(shortDataset.get(i)));
        }
        for (int i = 0; i < shortDataset.size(); i++) {
            conf.set("dataset-" + datasetId.get(shortDataset.get(i)) + "-agg-size",
                    Integer.toString(datasetAgg.get(shortDataset.get(i)).split(",").length));
        }
        conf.set("dataset-keys", datasetIds);
        conf.set("dataset-names", datasetNames);
        conf.set("first-group", firstGroupStr);
        conf.set("second-group", secondGroupStr);
        conf.set("main-dataset-id", datasetId.get(shortDataset.get(0)));
        if (noRelationship.length() > 0) {
            conf.set("no-relationship", noRelationship.substring(0, noRelationship.length()-1));
        }
        
        conf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.task.io.sort.mb", "200");
        conf.set("mapreduce.task.io.sort.factor", "100");
        conf.set("mapreduce.task.timeout", "2400000");
        
        if (s3) {
            machineConf.setMachineConfiguration(conf);
            conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
            conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
            conf.set("bucket", s3bucket);
        }
        
        if (snappyCompression) {
            conf.set("mapreduce.map.output.compress", "true");
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            //conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        }
        if (bzip2Compression) {
            conf.set("mapreduce.map.output.compress", "true");
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
            //conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        }
    
        Job job = new Job(conf);
        job.setJobName(jobName);
        
        job.setMapOutputKeyClass(PairAttributeWritable.class);
        job.setMapOutputValueClass(SpatioTemporalValueWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   
        job.setMapperClass(CorrelationTechniquesMapper.class);
        job.setReducerClass(CorrelationTechniquesReducer.class);
        job.setNumReduceTasks(machineConf.getNumberReduces());
   
        job.setInputFormatClass(SequenceFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
   
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, dataAttributesInputDirs.substring(0, dataAttributesInputDirs.length()-1));
        FileOutputFormat.setOutputPath(job, new Path(correlationOutputDir));
   
        job.setJarByClass(CorrelationTechniques.class);
   
        long start = System.currentTimeMillis();
        job.submit();
        job.waitForCompletion(true);
        System.out.println(jobName + "\t" + (System.currentTimeMillis() - start));
        
        // moving files to right place
        for (int i = 0; i < firstGroup.size(); i++) {
            for (int j = 0; j < secondGroup.size(); j++) {
                
                if (Integer.parseInt(datasetId.get(firstGroup.get(i))) < Integer.parseInt(datasetId.get(secondGroup.get(j)))) {
                    dataset1 = firstGroup.get(i);
                    dataset2 = secondGroup.get(j);
                } else {
                    dataset1 = secondGroup.get(j);
                    dataset2 = firstGroup.get(i);
                }
                
                if (dataset1.equals(dataset2)) continue;
                
                String from = s3bucket + FrameworkUtils.correlationTechniquesDir + "/tmp/" + dataset1 + "-" + dataset2 + "/";  
                String to = s3bucket + FrameworkUtils.correlationTechniquesDir + "/" + dataset1 + "-" + dataset2 + "/";
                FrameworkUtils.renameFile(from, to, s3conf, s3);
            }
        }    
    }

}