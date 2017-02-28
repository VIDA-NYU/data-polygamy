/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.text_output;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;

public class FeatureDataMapper extends Mapper<AttributeResolutionWritable, TopologyTimeSeriesWritable, Text, Text> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    String dataset = null;
    
    private MultipleOutputs<Text,Text> out;
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String[] fileSplitTokens = fileSplit.getPath().getParent().toString().split("/");
        dataset = fileSplitTokens[fileSplitTokens.length-1];
        
        out = new MultipleOutputs<Text,Text>(context);
    }
    
    @Override
    public void map(AttributeResolutionWritable key, TopologyTimeSeriesWritable value, Context context)
            throws IOException, InterruptedException {
        
        out.write(new Text(key.toString()), new Text(value.toString(key.getTemporalResolution())),
                generateFileName(dataset));
    }
    
    private String generateFileName(String dataset) {
        return (dataset + "/data");
    }
    
}
