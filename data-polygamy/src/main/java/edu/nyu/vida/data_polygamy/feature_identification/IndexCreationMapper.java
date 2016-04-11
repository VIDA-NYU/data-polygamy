/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.feature_identification;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.FloatArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalFloatWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

public class IndexCreationMapper extends Mapper<SpatioTemporalWritable, FloatArrayWritable, AttributeResolutionWritable, SpatioTemporalFloatWritable> {
    
    public static FrameworkUtils utils = new FrameworkUtils();
    
    String datasetIdStr = null;
    HashMap<String,String> datasetToId = new HashMap<String,String>();
    boolean useExistingMergeTree = false;
    
    int[] index;
    AttributeResolutionWritable keyWritable = new AttributeResolutionWritable();
    SpatioTemporalFloatWritable valueWritable = new SpatioTemporalFloatWritable();
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        String[] datasetNames = conf.get("dataset-name","").split(",");
        String[] datasetIds = conf.get("dataset-id","").split(",");
        for (int i = 0; i < datasetNames.length; i++)
            datasetToId.put(datasetNames[i], datasetIds[i]);
        
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String[] fileSplitTokens = fileSplit.getPath().getParent().toString().split("/");
        String dataset = fileSplitTokens[fileSplitTokens.length-1];
        datasetIdStr = datasetToId.get(dataset);
        
        String[] useMergeTree = conf.get("use-merge-tree","").split(",");
        for (String dt : useMergeTree) {
            if (dt.equals(dataset)) {
                useExistingMergeTree = true;
                break;
            }
        }
        
        String[] aggregates = context.getConfiguration().get("dataset-" + datasetIdStr +
                "-aggregates", "").split(",");
        
        index = new int[aggregates.length];
        for (int i = 0; i < aggregates.length; i++)
            index[i] = Integer.parseInt(aggregates[i].split("-")[0]);
    }
    
    @Override
    public void map(SpatioTemporalWritable key, FloatArrayWritable value, Context context)
            throws IOException, InterruptedException {
        
        if (!datasetIdStr.equals(String.valueOf(key.getDataset()))) {
            System.out.println("Something is wrong... Wrong dataset.");
            System.exit(-1);
        }
        
        if (useExistingMergeTree) {
            mapMergeTreeUsage(key, value, context);
        } else {
            mapMergeTreeCreation(key, value, context);
        }
    }
    
    /**
     * Map function if merge tree for the dataset must be created in reduce.
     */
    public void mapMergeTreeCreation(SpatioTemporalWritable key, FloatArrayWritable value, Context context)
            throws IOException, InterruptedException {
        
        float[] attributeValues = value.get();
        int spatial = key.getSpatial();
        int temporal = key.getTemporal();
        int spatialResolution = key.getSpatialResolution();
        int temporalResolution = key.getTemporalResolution();
        for (int i = 0; i < attributeValues.length; i++) {
            if (Float.isNaN(attributeValues[i]))
                continue;
            keyWritable = new AttributeResolutionWritable(index[i],
                    spatialResolution, temporalResolution, key.getDataset());
            valueWritable = new SpatioTemporalFloatWritable(spatial, temporal, attributeValues[i]);
            context.write(keyWritable, valueWritable);
        }
    }
    
    /**
     * Map function if merge tree for the dataset has already been created before.
     */
    public void mapMergeTreeUsage(SpatioTemporalWritable key, FloatArrayWritable value, Context context)
            throws IOException, InterruptedException {
        
        float[] attributeValues = value.get();
        int spatialResolution = key.getSpatialResolution();
        int temporalResolution = key.getTemporalResolution();
        for (int i = 0; i < index.length; i++) {
            if (Float.isNaN(attributeValues[i]))
                continue;
            keyWritable = new AttributeResolutionWritable(index[i],
                    spatialResolution, temporalResolution, key.getDataset());
            context.write(keyWritable, new SpatioTemporalFloatWritable());
        }
    }
    
}
