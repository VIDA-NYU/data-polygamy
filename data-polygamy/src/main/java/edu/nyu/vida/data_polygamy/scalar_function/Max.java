/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.collect.ComparisonChain;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;

public class Max extends Aggregation {
    
    private float max = Float.NEGATIVE_INFINITY;
    private int count = 0;
    
    public Max() {
        this.id = Function.MAX;
    }
    
    public float getMax() {
        return max;
    }
    
    @Override
    public void addValue(float value, int time) {
        if (Float.isNaN(value))
            return;
        
        if (value > max)
            max = value;
        count++;
    }
    
    @Override
    public void reset() {
        count = 0;
        max = Float.NEGATIVE_INFINITY;
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        if (count == 0)
            return Float.NaN;
        return max;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        max = in.readFloat();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(max);
        out.writeInt(count);
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Max agg = (Max) arg0;
        return ComparisonChain.start().
                compare(max, agg.getMax()).
                compare(count, agg.getCount()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Max))
            return false;
        return (this.compareTo((Max) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Max))
            throw new IllegalArgumentException("Invalid aggregation: expect max, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Max aggregation = (Max) agg;
        max = Math.max(max, aggregation.getMax());
        count += aggregation.getCount();
    }
}
