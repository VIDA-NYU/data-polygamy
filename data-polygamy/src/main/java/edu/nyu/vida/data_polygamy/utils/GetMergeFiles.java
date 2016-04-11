/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class GetMergeFiles { 

    public static void main(String[] args) throws IllegalArgumentException, IOException, URISyntaxException {
        String fromDirectory = args[0];
        String toEventsDirectory = args[1];
        String toOutliersDirectory = args[2];
        String metadataFile = args[3];
        
        // Detecting datasets.
        
        HashSet<String> datasets = new HashSet<String>();
        
        FileReader fileReader = new FileReader(metadataFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line;
        while((line = bufferedReader.readLine()) != null) {
            String[] parts = line.split(",");
            datasets.add(parts[0]);
        }    
        bufferedReader.close();
        
        // Downloading relationships.
        
        String relationshipPatternStr = "([a-zA-Z0-9]{4}\\-[a-zA-Z0-9]{4})\\-([a-zA-Z0-9]{4}\\-[a-zA-Z0-9]{4})";
        Pattern relationshipPattern = Pattern.compile(relationshipPatternStr);
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileSystem localFS = FileSystem.getLocal(conf);

        for (FileStatus status : fs.listStatus(new Path(fs.getHomeDirectory() + "/" + fromDirectory))) {
            if (!status.isDirectory()) {
                continue;
            }
            Path file = status.getPath();
            
            Matcher m = relationshipPattern.matcher(file.getName());
            if (!m.find()) continue;
            
            String ds1 = m.group(1);
            String ds2 = m.group(2);
            
            if (!datasets.contains(ds1)) continue;
            if (!datasets.contains(ds2)) continue;
            
            for (FileStatus statusDir : fs.listStatus(file)) {
                if (!statusDir.isDirectory()) {
                    continue;
                }
                
                Path fromPath = statusDir.getPath();
                String toPathStr;
                if (fromPath.getName().contains("events")) {
                    toPathStr = toEventsDirectory + "/" +
                            fromPath.getParent().getName() + "-" + fromPath.getName();
                } else {
                    toPathStr = toOutliersDirectory + "/" +
                            fromPath.getParent().getName() + "-" + fromPath.getName();
                }
                Path toPath = new Path(toPathStr);
                
                System.out.println("Copying:");
                System.out.println("  From: " + fromPath.toString());
                System.out.println("  To: " + toPath.toString());
                
                FileUtil.copyMerge(
                        fs, // HDFS File System
                        fromPath, // HDFS path
                        localFS, // Local File System
                        toPath, // Local Path
                        false, // Do not delete HDFS path
                        conf, // Configuration
                        null);
            }
        }
    }

}
