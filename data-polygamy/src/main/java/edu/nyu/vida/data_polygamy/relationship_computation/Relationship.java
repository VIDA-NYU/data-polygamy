/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.relationship_computation;

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
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;

public class Relationship {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    public static void addDatasets(String[] groupCmd, ArrayList<String> group,
            ArrayList<String> datasets, HashMap<String,String> datasetAgg,
            Path path, FileSystem fs, Configuration s3conf, boolean s3, String s3bucket) throws IOException {
        for (String dataset : groupCmd) {
            
            // getting indices
            
            String[] index = FrameworkUtils.searchIndex(dataset, s3conf, s3);
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
	    
	    Option scoreOption = new Option("sc", "score", true, "set threhsold for relationship score");
        scoreOption.setRequired(false);
        scoreOption.setArgName("SCORE THRESHOLD");
        options.addOption(scoreOption);
        
        Option strengthOption = new Option("st", "strength", true, "set threhsold for relationship strength");
        strengthOption.setRequired(false);
        strengthOption.setArgName("STRENGTH THRESHOLD");
        options.addOption(strengthOption);
        
        Option completeRandomizationOption = new Option("c", "complete-randomization", false,
                "use complete randomization when performing significance tests");
        completeRandomizationOption.setRequired(false);
        options.addOption(completeRandomizationOption);
        
        Option idOption = new Option("id", "ids", false,
                "output id instead of names for datasets and attributes");
        idOption.setRequired(false);
        options.addOption(idOption);
        
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
        
        Option tmpOption = new Option("tmp", "tmp", false, "leave files in tmp dir");
        tmpOption.setRequired(false);
        options.addOption(tmpOption);
        
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
        
        Option removeOption = new Option("r", "remove-not-significant", false, "remove relationships that are not"
                + "significant from the final output");
        removeOption.setRequired(false);
        options.addOption(removeOption);
        
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.relationship_computation.Relationship", options, true);
            System.exit(0);
        }
        
        if (cmd.hasOption("h")) {
            formatter.printHelp("hadoop jar data-polygamy.jar " +
                    "edu.nyu.vida.data_polygamy.relationship_computation.Relationship", options, true);
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
                    "edu.nyu.vida.data_polygamy.relationship_computation.Relationship", options, true);
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
    	
    	boolean removeNotSignificant = cmd.hasOption("r");
    	boolean removeExistingFiles = cmd.hasOption("f");
    	boolean completeRandomization = cmd.hasOption("c");
    	boolean hasScoreThreshold = cmd.hasOption("sc");
    	boolean hasStrengthThreshold = cmd.hasOption("st");
    	boolean outputIds = cmd.hasOption("id");
    	boolean tmp = cmd.hasOption("tmp");
    	String scoreThreshold = hasScoreThreshold ? cmd.getOptionValue("sc") : "";
    	String strengthThreshold = hasStrengthThreshold ? cmd.getOptionValue("st") : "";
    	
    	// all datasets
    	ArrayList<String> all_datasets = new ArrayList<String>();
    	if (s3) {
            path = new Path(s3bucket + FrameworkUtils.datasetsIndexDir);
            fs = FileSystem.get(path.toUri(), s3conf);
        } else {
            path = new Path (fs.getHomeDirectory() + "/" + FrameworkUtils.datasetsIndexDir);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null) {
            all_datasets.add(line.split("\t")[0]);
            line = br.readLine();
        }
        br.close();
        if (s3)
            fs.close();
        String[] all_datasets_array = new String[all_datasets.size()];
        all_datasets.toArray(all_datasets_array);
    	
    	String[] firstGroupCmd = cmd.getOptionValues("g1");
    	String[] secondGroupCmd = cmd.hasOption("g2") ? cmd.getOptionValues("g2") : all_datasets_array;
    	addDatasets(firstGroupCmd, firstGroup, shortDataset, datasetAgg, path, fs, s3conf, s3, s3bucket);
    	addDatasets(secondGroupCmd, secondGroup, shortDataset, datasetAgg, path, fs, s3conf, s3, s3bucket);
    	
    	if (shortDataset.size() == 0) {
    	    System.out.println("No datasets to process.");
    	    System.exit(0);
    	}
    	
    	if (firstGroup.isEmpty()) {
    	    System.out.println("No indices from datasets in G1.");
    	    System.exit(0);
    	}
    	
    	if (secondGroup.isEmpty()) {
    	    System.out.println("No indices from datasets in G2.");
            System.exit(0);
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
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        line = br.readLine();
        while (line != null) {
            String[] dt = line.split("\t");
            all_datasets.add(dt[0]);
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
        
        String relationshipsDir = "";
        if (outputIds) {
            relationshipsDir = FrameworkUtils.relationshipsIdsDir;
        } else {
            relationshipsDir = FrameworkUtils.relationshipsDir;
        }
        
        FrameworkUtils.createDir(s3bucket + relationshipsDir, s3conf, s3);
        
        String random = completeRandomization ? "complete" : "restricted";
        
        String indexInputDirs = "";
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
                String correlationOutputFileName = "";
                if (!tmp) {
                    correlationOutputFileName = s3bucket + relationshipsDir + "/" + dataset1 + "-" + dataset2 + "/";
                } else {
                    correlationOutputFileName = s3bucket + relationshipsDir + "/tmp/" + dataset1 + "-" + dataset2 + "/";
                }
                
                if (removeExistingFiles) {
                    FrameworkUtils.removeFile(correlationOutputFileName, s3conf, s3);
                }
                if (!FrameworkUtils.fileExists(correlationOutputFileName, s3conf, s3)) {
                    dirs.add(s3bucket + FrameworkUtils.indexDir + "/" + dataset1);
                    dirs.add(s3bucket + FrameworkUtils.indexDir + "/" + dataset2);
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
            indexInputDirs += dir + ",";
        }

        Configuration conf = new Configuration();
        Machine machineConf = new Machine(machine, nbNodes);
        
        String jobName = "relationship" + "-" + random;
        String relationshipOutputDir = s3bucket + relationshipsDir + "/tmp";
        
        FrameworkUtils.removeFile(relationshipOutputDir, s3conf, s3);
        
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
        conf.set("complete-random", String.valueOf(completeRandomization));
        conf.set("output-ids", String.valueOf(outputIds));
        conf.set("complete-random-str", random);
        conf.set("main-dataset-id", datasetId.get(shortDataset.get(0)));
        conf.set("remove-not-significant", String.valueOf(removeNotSignificant));
        if (noRelationship.length() > 0) {
            conf.set("no-relationship", noRelationship.substring(0, noRelationship.length()-1));
        }
        if (hasScoreThreshold) {
            conf.set("score-threshold", scoreThreshold);
        }
        if (hasStrengthThreshold) {
            conf.set("strength-threshold", strengthThreshold);
        }
        
        conf.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", String.valueOf(machineConf.getMaximumTasks()));
        conf.set("mapreduce.jobtracker.maxtasks.perjob", "-1");
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "20");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");
        conf.set("mapreduce.task.io.sort.mb", "200");
        conf.set("mapreduce.task.io.sort.factor", "100");
        conf.set("mapreduce.task.timeout", "2400000");

        //conf.set("mapreduce.map.memory.mb", "12000");
        //conf.set("mapreduce.map.java.opts", "-Xmx11000m");

        //conf.set("mapreduce.reduce.memory.mb", "20000");
        //conf.set("mapreduce.reduce.java.opts", "-Xmx12000m");
        machineConf.setMachineConfiguration(conf);
        //conf.set("mapreduce.map.memory.mb", "12000");
        //conf.set("mapreduce.map.java.opts", "-Xmx11000m");
        //conf.set("mapreduce.reduce.memory.mb", "50000");
        //conf.set("mapreduce.reduce.java.opts", "-Xmx40000m");
        //conf.set("mapreduce.task.timeout", "12000000");
        
        if (s3) {
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
        job.setMapOutputValueClass(TopologyTimeSeriesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   
        job.setMapperClass(CorrelationMapper.class);
        job.setReducerClass(CorrelationReducer.class);
        job.setNumReduceTasks(machineConf.getNumberReduces());
   
        job.setInputFormatClass(SequenceFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
   
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPaths(job, indexInputDirs.substring(0, indexInputDirs.length()-1));
        FileOutputFormat.setOutputPath(job, new Path(relationshipOutputDir));
   
        job.setJarByClass(Relationship.class);
   
        long start = System.currentTimeMillis();
        job.submit();
        job.waitForCompletion(true);
        System.out.println(jobName + "\t" + (System.currentTimeMillis() - start));
        
        // moving files to right place
        if (!tmp) {
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
                    
                    String from = s3bucket + relationshipsDir + "/tmp/" + dataset1 + "-" + dataset2 + "/";  
                    String to = s3bucket + relationshipsDir + "/" + dataset1 + "-" + dataset2 + "/";
                    FrameworkUtils.renameFile(from, to, s3conf, s3);
                }
            }  
        }
    }

}
