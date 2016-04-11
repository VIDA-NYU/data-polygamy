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

public class Min extends Aggregation {
    
    private float min = Float.POSITIVE_INFINITY;
    private int count = 0;
    
    public Min() {
        this.id = Function.MIN;
    }
    
    public float getMin() {
        return min;
    }
    
    @Override
    public void addValue(float value) {
        if (Float.isNaN(value))
            return;        
        if (value < min)
            min = value;
        count++;
    }
    
    @Override
    public void reset() {
        count = 0;
        min = Float.POSITIVE_INFINITY;
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        if (count == 0)
            return Float.NaN;
        return min;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readFloat();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(min);
        out.writeInt(count);
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Min agg = (Min) arg0;
        return ComparisonChain.start().
                compare(min, agg.getMin()).
                compare(count, agg.getCount()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Min))
            return false;
        return (this.compareTo((Min) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Min))
            throw new IllegalArgumentException("Invalid aggregation: expect min, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Min aggregation = (Min) agg;
        min = Math.min(min, aggregation.getMin());
        count += aggregation.getCount();
    }
}
