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

public class Gradient extends Aggregation {
    
    private int minTime = Integer.MAX_VALUE;
    private int maxTime = Integer.MIN_VALUE;
    private float minTimeValue = 0;
    private float maxTimeValue = 0;
    private int count = 0;
    
    public Gradient() {
        this.id = Function.GRADIENT;
    }
    
    public int getMinTime() {
        return minTime;
    }

    public int getMaxTime() {
        return maxTime;
    }

    public float getMinTimeValue() {
        return minTimeValue;
    }

    public float getMaxTimeValue() {
        return maxTimeValue;
    }

    @Override
    public void addValue(float value, int time) {
        if (Float.isNaN(value))
            return;
        
        if (time < minTime) {
            minTime = time;
            minTimeValue = value;
        }
        
        if (time > maxTime) {
            maxTime = time;
            maxTimeValue = value;
        }
        
        count++;
    }
    
    @Override
    public void reset() {
        minTime = Integer.MAX_VALUE;
        maxTime = Integer.MIN_VALUE;
        minTimeValue = 0;
        maxTimeValue = 0;
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
        if (maxTime - minTime == 0)
            return 0;
        return ((maxTimeValue - minTimeValue) / (maxTime - minTime)) * 1000000;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minTime = in.readInt();
        maxTime = in.readInt();
        minTimeValue = in.readFloat();
        maxTimeValue = in.readFloat();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(minTime);
        out.writeInt(maxTime);
        out.writeFloat(minTimeValue);
        out.writeFloat(maxTimeValue);
        out.writeInt(count);
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Gradient agg = (Gradient) arg0;
        return ComparisonChain.start().
                compare(minTime, agg.getMinTime()).
                compare(maxTime, agg.getMaxTime()).
                compare(minTimeValue, agg.getMinTimeValue()).
                compare(maxTimeValue, agg.getMaxTimeValue()).
                compare(count, agg.getCount()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Gradient))
            return false;
        return (this.compareTo((Gradient) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Gradient))
            throw new IllegalArgumentException("Invalid aggregation: expect gradient, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Gradient aggregation = (Gradient) agg;
        if (aggregation.getMinTime() < minTime) {
            minTime = aggregation.getMinTime();
            minTimeValue = aggregation.getMinTimeValue();
        }
        if (aggregation.getMaxTime() > maxTime) {
            maxTime = aggregation.getMaxTime();
            maxTimeValue = aggregation.getMaxTimeValue();
        }
        count += aggregation.getCount();
    }
}
