/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.text_output;

import java.io.IOException;
import java.util.ArrayList;
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

public class FeatureData {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    /**
     * @param args
     */
	@SuppressWarnings({"deprecation"})
	public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {
	    
	    Options options = new Options();
        
        Option forceOption = new Option("f", "force", false, "force the output "
                + "even if files already exist");
        forceOption.setRequired(false);
        options.addOption(forceOption);
        
        Option gOption = new Option("g", "group", true, "set group of datasets for which the output"
                + " will be generated");
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
                    "edu.nyu.vida.data_polygamy.text_output.FeatureData", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.text_output.FeatureData", options, true);
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
                        "edu.nyu.vida.data_polygamy.text_output.FeatureData", options, true);
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
    	
    	ArrayList<String> shortDataset = new ArrayList<String>();
    	ArrayList<String> shortDatasetIndex = new ArrayList<String>();
        
        boolean removeExistingFiles = cmd.hasOption("f");
        
        for (String dataset : cmd.getOptionValues("g")) {

            // getting features
            String[] index = FrameworkUtils.searchIndex(dataset, s3conf, s3);
            if (index.length == 0) {
                System.out.println("No index found for " + dataset + ".");
                continue;
            }
            
            shortDataset.add(dataset);
        }
        
        if (shortDataset.size() == 0) {
            System.out.println("No datasets to process.");
            System.exit(0);
        }
        
        HashSet<String> input = new HashSet<String>();
        
        for (String dataset: shortDataset) {
            
            String indexTextOutputFileName = s3bucket + FrameworkUtils.indexTextDir + "/" + dataset + "/";
            
            if (removeExistingFiles) {
                FrameworkUtils.removeFile(indexTextOutputFileName, s3conf, s3);
            }
            
            if (!FrameworkUtils.fileExists(indexTextOutputFileName, s3conf, s3)) {
                input.add(s3bucket + FrameworkUtils.indexDir + "/" + dataset);
                shortDatasetIndex.add(dataset);
            }
            
        }
        
        if (input.isEmpty()) {
            System.out.println("All the input datasets have been outputted in text format.");
            System.out.println("Use -f in the beginning of the command line to force the computation.");
            System.exit(0);
        }
        
        String indexDatasets = "";
        Iterator<String> it = input.iterator();
        while (it.hasNext()) {
            indexDatasets += it.next() + ",";
        }
        
    	Job job = null;
        Configuration conf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        
        String jobName = "output-to-text";
        String indexOutputDir = s3bucket + FrameworkUtils.indexTextDir + "/tmp/";
        
        FrameworkUtils.removeFile(indexOutputDir, s3conf, s3);
        
        conf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.task.io.sort.mb", "200");
        conf.set("mapreduce.task.io.sort.factor", "100");
        //conf.set("mapreduce.task.timeout", "1800000");
        machineConf.setMachineConfiguration(conf);

        conf.set("mapreduce.map.memory.mb", "50000");
        conf.set("mapreduce.map.java.opts", "-Xmx40000m");
        conf.set("mapreduce.task.timeout", "12000000");
        
        if (s3) {
            //machineConf.setMachineConfiguration(conf);
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
        
        job = new Job(conf);
        job.setJobName(jobName);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   
        job.setMapperClass(FeatureDataMapper.class);
        job.setNumReduceTasks(0);
   
        job.setInputFormatClass(SequenceFileInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
   
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, indexDatasets.substring(0,
                indexDatasets.length()-1));
        FileOutputFormat.setOutputPath(job, new Path(indexOutputDir));
   
        job.setJarByClass(FeatureData.class);
        
        long start = System.currentTimeMillis();
        job.submit();
        job.waitForCompletion(true);
        System.out.println(jobName + "\t" + (System.currentTimeMillis() - start));
        
        // moving files to right place
        for (String dataset: shortDatasetIndex) {
            String from = s3bucket + FrameworkUtils.indexTextDir + "/tmp/" + dataset + "/";
            String to = s3bucket + FrameworkUtils.indexTextDir + "/" + dataset + "/";
            FrameworkUtils.renameFile(from, to, s3conf, s3);
        }
        
    }

}
