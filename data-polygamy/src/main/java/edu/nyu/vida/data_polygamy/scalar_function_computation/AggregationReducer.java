/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function_computation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.nyu.vida.data_polygamy.scalar_function.Aggregation;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.FloatArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

//public class AggregationReducer extends Reducer<SpatioTemporalWritable, AggregationArrayWritable, Text, Text> {
public class AggregationReducer extends Reducer<SpatioTemporalWritable, AggregationArrayWritable, SpatioTemporalWritable, FloatArrayWritable> {
    
	public static FrameworkUtils utils = new FrameworkUtils();
	
	HashMap<Integer,String> idToDataset = new HashMap<Integer,String>();
	
    FloatArrayWritable valueWritable;
    float[] output;
    
    private MultipleOutputs<SpatioTemporalWritable,FloatArrayWritable> out;
    //private MultipleOutputs<Text,Text> out;
    
    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        String[] datasetNames = context.getConfiguration().get("dataset-name","").split(",");
        String[] datasetIds = context.getConfiguration().get("dataset-id","").split(",");
        for (int i = 0; i < datasetNames.length; i++)
            idToDataset.put(Integer.parseInt(datasetIds[i]), datasetNames[i]);
        out = new MultipleOutputs<SpatioTemporalWritable,FloatArrayWritable>(context);
        //out = new MultipleOutputs<Text,Text>(context);
    }

    @Override
    public void reduce(SpatioTemporalWritable key, Iterable<AggregationArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        
        Aggregation[] aggregates = null;
        boolean init = false;
        
        Iterator<AggregationArrayWritable> it = values.iterator();
        while (it.hasNext()) {
            Aggregation[] val = it.next().get();
            if (!init) {
                aggregates = new Aggregation[val.length];
                for (int i = 0; i < val.length; i++) {
                    Function function = val[i].getId();
                    aggregates[i] = FrameworkUtils.getAggregation(function);
                    aggregates[i].add(val[i]);
                }
                init = true;
            } else {
                for (int i = 0; i < val.length; i++)
                    aggregates[i].add(val[i]);
            }
        }

        output = new float[aggregates.length];
        for (int i = 0; i < aggregates.length; i++)
            output[i] = aggregates[i].getResult();
        valueWritable = new FloatArrayWritable(output);
        //out.write(new Text(key.toString()), new Text(valueWritable.toString()),
        //        generateFileName(idToDataset.get(key.getDataset())));
        out.write(key, valueWritable,
                generateFileName(idToDataset.get(key.getDataset())));
        //context.write(key, valueWritable);
        
        for (int i = 0; i < aggregates.length; i++)
            aggregates[i].reset();
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    	out.close();
    }
    
    private String generateFileName(String dataset) {
    	
    	return (dataset + "/data");
    }
}
