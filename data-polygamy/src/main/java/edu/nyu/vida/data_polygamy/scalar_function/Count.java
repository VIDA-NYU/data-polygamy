/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;

public class Count extends Aggregation {
    
    private int count = 0;
    
    public Count() {
        this.id = Function.COUNT;
    }
    
    @Override
    public void addValue(float value, int time) {
        count++;
    }
    
    @Override
    public void reset() {
        count = 0;
    }
    
    @Override
    public int getCount() {
        return count;
    }
    
    @Override
    public float getResult() {
        return count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Count agg = (Count) arg0;
//        Integer countObj = new Integer(count);
//        return countObj.compareTo(agg.getCount());
        int c = agg.getCount();
        return (count < c) ? -1 : ((count == c) ? 0 : 1);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Count))
            return false;
        return (this.compareTo((Count) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof Count))
            throw new IllegalArgumentException("Invalid aggregation: expect count, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        Count aggregation = (Count) agg;
        count += aggregation.getCount();
    }
}
