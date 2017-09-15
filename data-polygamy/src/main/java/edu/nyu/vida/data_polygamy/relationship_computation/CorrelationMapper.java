/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.relationship_computation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AttributeResolutionWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.PairAttributeWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;

public class CorrelationMapper extends Mapper<AttributeResolutionWritable, TopologyTimeSeriesWritable, PairAttributeWritable, TopologyTimeSeriesWritable> {
    
	public static FrameworkUtils utils = new FrameworkUtils();
	
    HashMap<Integer,Integer> datasetAggSize = new HashMap<Integer,Integer>();
    HashSet<Integer> firstGroup = new HashSet<Integer>();
    HashSet<Integer> secondGroup = new HashSet<Integer>();
    
    PairAttributeWritable keyWritable = new PairAttributeWritable();

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        
        Configuration conf = context.getConfiguration();
        
        String[] datasetIdsStr = conf.get("dataset-keys","").split(",");
        for (int i = 0; i < datasetIdsStr.length; i++) {
            datasetAggSize.put(Integer.parseInt(datasetIdsStr[i]),
                    Integer.parseInt(conf.get("dataset-" + datasetIdsStr[i] + "-agg-size","0")));
        }
        
        String[] firstGroupStr = conf.get("first-group","").split(",");
        String[] secondGroupStr = conf.get("second-group","").split(",");
        for (String dataset : firstGroupStr) {
            firstGroup.add(Integer.parseInt(dataset));
        }
        for (String dataset : secondGroupStr) {
            secondGroup.add(Integer.parseInt(dataset));
        }
    }
    
    @Override
    public void map(AttributeResolutionWritable key, TopologyTimeSeriesWritable value, Context context)
            throws IOException, InterruptedException {
        
        int attribute = key.getAttribute();
        int dataset = key.getDataset();
        
        if (firstGroup.contains(dataset))
            generateAllAttributePairs(key, value, context, attribute, dataset, secondGroup);
        else if (secondGroup.contains(dataset))
            generateAllAttributePairs(key, value, context, attribute, dataset, firstGroup);
        else {
            System.out.println("Something went wrong... Dataset id not found in any group.");
            System.exit(-1);
        }
    }
    
    public void generateAllAttributePairs(AttributeResolutionWritable key, TopologyTimeSeriesWritable value,
            Context context, int attribute, int dataset, HashSet<Integer> datasetIds) throws IOException, InterruptedException {
        int dataset1 = 0;
        int dataset2 = 0;
        
        for (int compareDataset : datasetIds) {
            if (compareDataset == dataset) continue;
            ArrayList<Integer> pairs = new ArrayList<Integer>();
            int compareAggSize = datasetAggSize.get(compareDataset);
            if (dataset < compareDataset) { // first position
                dataset1 = dataset;
                dataset2 = compareDataset;
                for (int i = 0; i < compareAggSize; i++) {
                    pairs.add(attribute);
                    pairs.add(i);
                }
            } else { // second position
                dataset1 = compareDataset;
                dataset2 = dataset;
                for (int i = 0; i < compareAggSize; i++) {
                    pairs.add(i);
                    pairs.add(attribute);
                }
            }
            
            for (int i = 0; i < pairs.size(); i+=2) {
                keyWritable = new PairAttributeWritable(pairs.get(i), pairs.get(i+1),
                        dataset1, dataset2,
                        key.getSpatialResolution(), key.getTemporalResolution(), value.getIsOutlier());
                context.write(keyWritable, value);
            }
        }
    }
}
