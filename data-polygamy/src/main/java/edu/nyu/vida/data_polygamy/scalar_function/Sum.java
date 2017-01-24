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

public class Sum extends Aggregation {

    private float sum = 0;
    private int count = 0;
    
    public Sum() {
        this.id = Function.SUM;
    }
    
    public float getSum() {
        return sum;
    }
    
    @Override
    public void addValue(float value, int time) {
        if (Float.isNaN(value))
            return;        
        sum += value;
        count++;
    }
    
    @Override
    public void reset() {
        sum = 0;
        count = 0;
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        if (count == 0)
            return Float.NaN;
        return sum;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readFloat();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(sum);
        out.writeInt(count);
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Sum agg = (Sum) arg0;
        return ComparisonChain.start().
                compare(sum, agg.getSum()).
                compare(count, agg.getCount()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Sum))
            return false;
        return (this.compareTo((Sum) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Sum))
            throw new IllegalArgumentException("Invalid aggregation: expect sum, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Sum aggregation = (Sum) agg;
        sum += aggregation.getSum();
        count += aggregation.getCount();
    }
}
