/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;

public class Median extends Aggregation {
    
    private int count = 0;
    private ArrayList<Float> floatValues = new ArrayList<Float>();
    
    public Median() {
        this.id = Function.MEDIAN;
    }
    
    public ArrayList<Float> getValues() {
        return floatValues;
    }
    
    @Override
    public void addValue(float value, int time) {
        if (Float.isNaN(value))
            return;
        
        floatValues.add(value);
        count++;
    }
    
    @Override
    public void reset() {
        count = 0;
        floatValues.clear();
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        if (count == 0)
            return Float.NaN;
        
        double[] primitiveValues = new double[floatValues.size()];
        for (int i = 0; i < floatValues.size(); i++)
            primitiveValues[i] = floatValues.get(i);
        DescriptiveStatistics stats = new DescriptiveStatistics(primitiveValues);
        
        return (float) stats.getPercentile(50);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        int size = in.readInt();
        floatValues = new ArrayList<Float>(size);
        for (int i = 0; i < size; i++)
            floatValues.add(in.readFloat());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(floatValues.size());
        for (int i = 0; i < floatValues.size(); i++)
            out.writeFloat(floatValues.get(i));
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Median agg = (Median) arg0;
        Integer countObj = new Integer(count);
        return countObj.compareTo(agg.getCount());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Median))
            return false;
        return (this.compareTo((Median) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Median))
            throw new IllegalArgumentException("Invalid aggregation: expect median, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Median aggregation = (Median) agg;
        floatValues.addAll(aggregation.getValues());
        count += aggregation.getCount();
    }
}
