/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.standard_techniques;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.FloatArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.PairAttributeWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalValueWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

public class CorrelationTechniquesMapper extends Mapper<SpatioTemporalWritable, FloatArrayWritable, PairAttributeWritable, SpatioTemporalValueWritable> {
    
	public static FrameworkUtils utils = new FrameworkUtils();
	
	String datasetIdStr = null;
    HashMap<String,String> datasetToId = new HashMap<String,String>();
    
    int[] index;
    HashMap<Integer,Integer> datasetAggSize = new HashMap<Integer,Integer>();
    HashSet<Integer> firstGroup = new HashSet<Integer>();
    HashSet<Integer> secondGroup = new HashSet<Integer>();
    HashSet<String> noRelationship = new HashSet<String>();
    
    PairAttributeWritable keyWritable = new PairAttributeWritable();
    SpatioTemporalValueWritable valueWritable = new SpatioTemporalValueWritable();

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        String[] datasetNames = conf.get("dataset-names","").split(",");
        String[] datasetIds = conf.get("dataset-keys","").split(",");
        for (int i = 0; i < datasetNames.length; i++)
            datasetToId.put(datasetNames[i], datasetIds[i]);
        
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String[] fileSplitTokens = fileSplit.getPath().getParent().toString().split("/");
        String dataset = fileSplitTokens[fileSplitTokens.length-1];
        datasetIdStr = datasetToId.get(dataset);
        
        String[] aggregates = context.getConfiguration().get("dataset-" + datasetIdStr +
                "-agg", "").split(",");
        
        index = new int[aggregates.length];
        for (int i = 0; i < aggregates.length; i++)
            index[i] = Integer.parseInt(aggregates[i].split("-")[0]);
        
        for (int i = 0; i < datasetIds.length; i++) {
            datasetAggSize.put(Integer.parseInt(datasetIds[i]),
                    Integer.parseInt(conf.get("dataset-" + datasetIds[i] + "-agg-size","0")));
        }
        
        if (conf.get("no-relationship", "").length() > 0) {
            String[] noRelationshipStr = conf.get("no-relationship").split(",");
            for (String relationship : noRelationshipStr) {
                String[] ids = relationship.split("-");
                if (Integer.parseInt(ids[0]) < Integer.parseInt(ids[1])) {
                    noRelationship.add(relationship);
                } else {
                    noRelationship.add(ids[1] + "-" + ids[0]);
                }
            }
        }
        
        String[] firstGroupStr = conf.get("first-group","").split(",");
        String[] secondGroupStr = conf.get("second-group","").split(",");
        for (String dt : firstGroupStr) {
            firstGroup.add(Integer.parseInt(dt));
        }
        for (String dt : secondGroupStr) {
            secondGroup.add(Integer.parseInt(dt));
        }
    }
    
    @Override
    public void map(SpatioTemporalWritable key, FloatArrayWritable value, Context context)
            throws IOException, InterruptedException {
        
        int dataset = key.getDataset();
        
        if (!datasetIdStr.equals(String.valueOf(dataset))) {
            System.out.println("Something is wrong... Wrong dataset.");
            System.exit(-1);
        }
        
        int spatialResolution = key.getSpatialResolution();
        int temporalResolution = key.getTemporalResolution();
        
        switch(spatialResolution) {
        case FrameworkUtils.NBHD:
            return;
        case FrameworkUtils.ZIP:
            return;
        case FrameworkUtils.GRID:
            return;
        case FrameworkUtils.CITY:
            break;
        default:
            break;
        }
        
        float[] attributeValues = value.get();
        int spatial = key.getSpatial();
        int temporal = key.getTemporal();
        for (int i = 0; i < attributeValues.length; i++) {
            if (Float.isNaN(attributeValues[i]))
                continue;
            int attribute = index[i];
            if (firstGroup.contains(dataset))
                generateAllAttributePairs(dataset, attribute, spatialResolution, temporalResolution,
                        spatial, temporal, attributeValues[i], context, secondGroup);
            else if (secondGroup.contains(dataset))
                generateAllAttributePairs(dataset, attribute, spatialResolution, temporalResolution,
                        spatial, temporal, attributeValues[i], context, firstGroup);
            else {
                System.out.println("Something went wrong... Dataset id not found in any group.");
                System.exit(-1);
            }
            
        }
    }
    
    public void generateAllAttributePairs(int dataset, int attribute, int spatialResolution, int temporalResolution,
            int spatial, int temporal, float value, Context context, HashSet<Integer> datasetIds)
                    throws IOException, InterruptedException {
        int dataset1 = 0;
        int dataset2 = 0;
        
        for (int compareDataset : datasetIds) {
            if (compareDataset == dataset) continue;
            ArrayList<Integer> pairs = new ArrayList<Integer>();
            int compareAggSize = datasetAggSize.get(compareDataset);
            if (dataset < compareDataset) { // first position
                dataset1 = dataset;
                dataset2 = compareDataset;
                String relationship = Integer.toString(dataset1) + "-" + Integer.toString(dataset2);
                if (noRelationship.contains(relationship))
                    continue;
                for (int i = 0; i < compareAggSize; i++) {
                    pairs.add(attribute);
                    pairs.add(i);
                }
            } else { // second position
                dataset1 = compareDataset;
                dataset2 = dataset;
                String relationship = Integer.toString(dataset1) + "-" + Integer.toString(dataset2);
                if (noRelationship.contains(relationship))
                    continue;
                for (int i = 0; i < compareAggSize; i++) {
                    pairs.add(i);
                    pairs.add(attribute);
                }
            }
            
            for (int i = 0; i < pairs.size(); i+=2) {
                keyWritable = new PairAttributeWritable(pairs.get(i), pairs.get(i+1),
                        dataset1, dataset2,
                        spatialResolution, temporalResolution, false);
                valueWritable = new SpatioTemporalValueWritable(spatial, temporal, dataset, attribute, value);
                context.write(keyWritable, valueWritable);
            }
        }
    }
}
