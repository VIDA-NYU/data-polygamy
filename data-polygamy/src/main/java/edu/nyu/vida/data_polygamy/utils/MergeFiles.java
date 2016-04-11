/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.utils;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.FloatArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;

public class MergeFiles {
    
    public static <K, V> void merge(Path fromDirectory,
            Path toFile, Class<K> keyClass, Class<V> valueClass) throws IOException {
        
        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(toFile),
                SequenceFile.Writer.keyClass(keyClass),
                SequenceFile.Writer.valueClass(valueClass)
                );

        for (FileStatus status : fs.listStatus(fromDirectory)) {
            if (status.isDirectory()) {
                System.out.println("Skip directory '" + status.getPath().getName() + "'");
                continue;
            }

            Path file = status.getPath();

            if (file.getName().startsWith("_")) {
                System.out.println("Skip \"_\"-file '" + file.getName() + "'"); //There are files such "_SUCCESS"-named in jobs' ouput folders 
                continue;
            }

            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                writer.append(key, value);
            }

            reader.close();
        }

        writer.close();
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, URISyntaxException {
        String fromDirectory = args[0];
        String toFile = args[1];
        String pipelinePhase = args[2];
        
        if (pipelinePhase.equals("aggregates")) {
            MergeFiles.merge(new Path(fromDirectory),
                    new Path(toFile),
                    SpatioTemporalWritable.class,
                    FloatArrayWritable.class);
        } else if (pipelinePhase.equals("index")) {
            MergeFiles.merge(new Path(fromDirectory),
                    new Path(toFile),
                    AttributeResolutionWritable.class,
                    TopologyTimeSeriesWritable.class);
        } else {
            System.out.println("Invalid phase: " + pipelinePhase);
            System.exit(0);
        }
    }

}
