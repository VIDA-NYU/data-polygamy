/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.pre_processing;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Machine;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.MultipleSpatioTemporalWritable;

public class PreProcessing {
    
    public static FrameworkUtils utils = new FrameworkUtils();

    /**
     * @param args
     * @throws IOException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {
	    
	    Options options = new Options();
        
        Option nameOption = new Option("dn", "name", true, "the name of the dataset");
        nameOption.setRequired(true);
        nameOption.setArgName("DATASET NAME");
        options.addOption(nameOption);
        
        Option headerOption = new Option("dh", "header", true, "the file that contains the header of the dataset");
        headerOption.setRequired(true);
        headerOption.setArgName("DATASET HEADER FILE");
        options.addOption(headerOption);
        
        Option deafultsOption = new Option("dd", "defaults", true, "the file that contains the default values of the dataset");
        deafultsOption.setRequired(true);
        deafultsOption.setArgName("DATASET DEFAULTS FILE");
        options.addOption(deafultsOption);
        
        Option tempResOption = new Option("t", "temporal", true, "desired temporal resolution (hour, day, week, or month)");
        tempResOption.setRequired(true);
        tempResOption.setArgName("TEMPORAL RESOLUTION");
        options.addOption(tempResOption);
        
        Option spatialResOption = new Option("s", "spatial", true, "desired spatial resolution (points, nbhd, zip, grid, or city)");
        spatialResOption.setRequired(true);
        spatialResOption.setArgName("SPATIAL RESOLUTION");
        options.addOption(spatialResOption);
        
        Option currentSpatialResOption = new Option("cs", "current-spatial", true, "current spatial resolution (points, nbhd, zip, grid, or city)");
        currentSpatialResOption.setRequired(true);
        currentSpatialResOption.setArgName("CURRENT SPATIAL RESOLUTION");
        options.addOption(currentSpatialResOption);
        
        Option indexResOption = new Option("i", "index", true, "indexes of the temporal and spatial attributes");
        indexResOption.setRequired(true);
        indexResOption.setArgName("INDEX OF SPATIO-TEMPORAL RESOLUTIONS");
        indexResOption.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(indexResOption);
        
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
                    "edu.nyu.vida.data_polygamy.pre_processing.PreProcessing", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.pre_processing.PreProcessing", options, true);
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
                        "edu.nyu.vida.data_polygamy.pre_processing.PreProcessing", options, true);
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
        
        Configuration conf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        String dataset = cmd.getOptionValue("dn");
        String header = cmd.getOptionValue("dh");
        String defaults = cmd.getOptionValue("dd");
        String temporalResolution = cmd.getOptionValue("t");
        String spatialResolution = cmd.getOptionValue("s");
        String gridResolution = "";
        String currentSpatialResolution = cmd.getOptionValue("cs");
        
        if (spatialResolution.contains("grid")) {
            String[] res = spatialResolution.split("-");
            spatialResolution = res[0];
            gridResolution = res[1];
        }
        
        conf.set("header", s3bucket + FrameworkUtils.dataDir + "/" + header);
        conf.set("defaults", s3bucket + FrameworkUtils.dataDir + "/" + defaults);
        conf.set("temporal-resolution", temporalResolution);
        conf.set("spatial-resolution", spatialResolution);
        conf.set("grid-resolution", gridResolution);
        conf.set("current-spatial-resolution", currentSpatialResolution);
        
        String[] indexes = cmd.getOptionValues("i");
        String temporalPos = "";
        Integer sizeSpatioTemp = 0;
        if (!(currentSpatialResolution.equals("points"))) {
            String spatialPos = "";
            for (int i = 0; i < indexes.length; i++) {
                temporalPos += indexes[i] + ",";
                spatialPos += indexes[++i] + ",";
                sizeSpatioTemp++;
            }
            conf.set("spatial-pos", spatialPos);
        } else {
            String xPositions = "", yPositions = "";
            for (int i = 0; i < indexes.length; i++) {
                temporalPos += indexes[i] + ",";
                xPositions += indexes[++i] + ",";
                yPositions += indexes[++i] + ",";
                sizeSpatioTemp++;
            }
            conf.set("xPositions", xPositions);
            conf.set("yPositions", yPositions);
        }
        conf.set("temporal-pos", temporalPos);
        
        conf.set("size-spatio-temporal", sizeSpatioTemp.toString());
        
        // checking resolutions
        
        if (utils.spatialResolution(spatialResolution) < 0) {
            System.out.println("Invalid spatial resolution: " + spatialResolution);
            System.exit(-1);
        }
        
        if (utils.spatialResolution(spatialResolution) == FrameworkUtils.POINTS) {
            System.out.println("The data needs to be reduced at least to neighborhoods or grid.");
            System.exit(-1);
        }
        
        if (utils.spatialResolution(currentSpatialResolution) < 0) {
            System.out.println("Invalid spatial resolution: " + currentSpatialResolution);
            System.exit(-1);
        }
        
        if (utils.spatialResolution(currentSpatialResolution) > utils.spatialResolution(spatialResolution)) {
            System.out.println("The current spatial resolution is coarser than " +
            		"the desired one. You can only navigate from a fine resolution" +
            		" to a coarser one.");
            System.exit(-1);
        }
        
        if (utils.temporalResolution(temporalResolution) < 0) {
            System.out.println("Invalid temporal resolution: " + temporalResolution);
            System.exit(-1);
        }
        
        String fileName = s3bucket + FrameworkUtils.preProcessingDir + "/" + dataset + "-" + temporalResolution + "-" + spatialResolution + gridResolution;
        conf.set("aggregates", fileName + ".aggregates");
        
        // making sure both files are removed, if they exist
        FrameworkUtils.removeFile(fileName, s3conf, s3);
        FrameworkUtils.removeFile(fileName + ".aggregates", s3conf, s3);
        
        /**
         * Hadoop Parameters
         * sources: http://www.slideshare.net/ImpetusInfo/ppt-on-advanced-hadoop-tuning-n-optimisation
         *          https://cloudcelebrity.wordpress.com/2013/08/14/12-key-steps-to-keep-your-hadoop-cluster-running-strong-and-performing-optimum/
         */
        
        conf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.task.io.sort.mb", "200");
        conf.set("mapreduce.task.io.sort.factor", "100");
        
        // using SnappyCodec for intermediate and output data ?
        // TODO: for now, using SnappyCodec -- what about LZO + Protocol Buffer serialization?
        //   LZO - http://www.oberhumer.com/opensource/lzo/#download
        //   Hadoop-LZO - https://github.com/twitter/hadoop-lzo
        //   Protocol Buffer - https://github.com/twitter/elephant-bird
        //   General Info - http://www.devx.com/Java/Article/47913
        //   Compression - http://comphadoop.weebly.com/index.html
        if (snappyCompression) {
	        conf.set("mapreduce.map.output.compress", "true");
	        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
	        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        }
        if (bzip2Compression) {
            conf.set("mapreduce.map.output.compress", "true");
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
            conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        }
        
        // TODO: this is dangerous!
        if (s3) {
            conf.set("fs.s3.awsAccessKeyId", awsAccessKeyId);
            conf.set("fs.s3.awsSecretAccessKey", awsSecretAccessKey);
            conf.set("bucket", s3bucket);
        }
        
        Job job = new Job(conf);
        job.setJobName(dataset + "-" + temporalResolution + "-" + spatialResolution);
        
        job.setMapOutputKeyClass(MultipleSpatioTemporalWritable.class);
        job.setMapOutputValueClass(AggregationArrayWritable.class);
        
        job.setOutputKeyClass(MultipleSpatioTemporalWritable.class);
        job.setOutputValueClass(AggregationArrayWritable.class);
   
        job.setMapperClass(PreProcessingMapper.class);
        job.setCombinerClass(PreProcessingCombiner.class);
        job.setReducerClass(PreProcessingReducer.class);
        job.setNumReduceTasks(machineConf.getNumberReduces());
        //job.setNumReduceTasks(1);
   
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

        FileInputFormat.setInputPaths(job, new Path(s3bucket + FrameworkUtils.dataDir + "/" + dataset));
        FileOutputFormat.setOutputPath(job, new Path(fileName));
   
        job.setJarByClass(PreProcessing.class);
   
        long start = System.currentTimeMillis();
        job.submit();
        job.waitForCompletion(true);
        System.out.println(fileName + "\t" + (System.currentTimeMillis() - start));

    }

}
