/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;

public class Unique extends Aggregation {
    
    private int count = 0;
    private HashSet<Float> values = new HashSet<Float>();
    
    public Unique() {
        this.id = Function.UNIQUE;
    }
    
    public HashSet<Float> getValues() {
        return values;
    }
    
    @Override
    public void addValue(float value) {
        if (Float.isNaN(value))
            return;        
        values.add(value);
        count++;
    }
    
    @Override
    public void reset() {
        count = 0;
        values.clear();
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        if (count == 0)
            return Float.NaN;
        return (float)values.size();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        int size = in.readInt();
        values = new HashSet<Float>(size);
        Float val;
        for (int i = 0; i < size; i++) {
            val = in.readFloat();
            values.add(val);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(values.size());
        Iterator<Float> it = values.iterator();
        while (it.hasNext()) {
            Float val = it.next();
            out.writeFloat(val);
        }
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Unique agg = (Unique) arg0;
        return ComparisonChain.start().
                compare(values.size(), agg.getValues().size()).
                compare(count, agg.getCount()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Unique))
            return false;
        return (this.compareTo((Unique) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Unique))
            throw new IllegalArgumentException("Invalid aggregation: expect unique, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Unique aggregation = (Unique) agg;
        values.addAll(aggregation.getValues());
        count += aggregation.getCount();
    }
}
