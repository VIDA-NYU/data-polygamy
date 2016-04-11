/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function_computation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import edu.nyu.vida.data_polygamy.scalar_function.Aggregation;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.AggregationArrayWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.SpatioTemporalWritable;

public class AggregationCombiner extends Reducer<SpatioTemporalWritable, AggregationArrayWritable, SpatioTemporalWritable, AggregationArrayWritable> {
    
    AggregationArrayWritable valueWritable = new AggregationArrayWritable();

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

        valueWritable = new AggregationArrayWritable(aggregates);
        //context.write(new Text(key.toString()), new Text(valueWritable.toString()));
        context.write(key, valueWritable);
        
        for (int i = 0; i < aggregates.length; i++)
            aggregates[i].reset();
    }

}
