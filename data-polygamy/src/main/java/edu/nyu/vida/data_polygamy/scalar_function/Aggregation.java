/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;

public abstract class Aggregation {
    
    protected Function id = Function.NONE;
    
    public Aggregation() {}
    
    abstract public void addValue(float value, int time);
    abstract public void reset();
    abstract public int getCount();
    abstract public float getResult();
    abstract public void add(Aggregation agg);
    
    abstract public void readFields(DataInput in) throws IOException;
    abstract public void write(DataOutput out) throws IOException;
    abstract public int compareTo(Aggregation arg0);
    abstract public boolean equals(Object o);
    
    public Function getId() {
        return id;
    }
}
