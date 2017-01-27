/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ComparisonChain;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Vector;

public class CountGradient extends Aggregation {
    
    private HashMap<Integer, Integer> values = new HashMap<Integer, Integer>();
    
    public CountGradient() {
        this.id = Function.COUNT_GRADIENT;
    }
    
    public HashMap<Integer, Integer> getValues() {
        return values;
    }
    
    private float getDirection() {
        ArrayList<Vector> vectorArray = new ArrayList<Vector>(); 
        
        Float lastX = null;
        Float lastY = null;
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        for (Integer key : keys) {
            if (lastX == null) {
                lastX = (float) key;
                lastY = (float) values.get(key);
                continue;
            }
            float currentX = (float) key;
            float currentY = (float) values.get(key);
            Vector vector = new Vector(lastX, lastY, currentX, currentY);
            vectorArray.add(vector);
            lastX = currentX;
            lastY = currentY;
        }
        
        float x1 = 0;
        float x2 = 0;
        float y1 = 0;
        float y2 = 0;
        int size = vectorArray.size();
        for (Vector vector : vectorArray) {
            x1 += vector.getX1();
            x2 += vector.getX2();
            y1 += vector.getY1();
            y2 += vector.getY2();
        }
        x1 = x1 / size;
        x2 = x2 / size;
        y1 = y1 / size;
        y2 = y2 / size;
        
        Vector finalVector = new Vector(x1, y1, x2, y2);
        return finalVector.getDirection();
    }

    @Override
    public void addValue(float value, int time) {
        if (Float.isNaN(value))
            return;
        
        Integer timeVal = 1;
        if (values.containsKey(time)) {
            timeVal += values.get(time);
        }
        
        values.put(time, timeVal);
    }
    
    @Override
    public void reset() {
        values = new HashMap<Integer, Integer>();
    }
    
    @Override
    public int getCount() {
        return values.size();
    }
    
    @Override
    public float getResult() {
        if (values.size() == 0)
            return Float.NaN;
        if (values.size() == 1)
            return 0;
        return this.getDirection();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        values = new HashMap<Integer, Integer>(size);
        for (int i = 0; i < size; i++) {
            Integer time = in.readInt();
            Integer val = in.readInt();
            values.put(time, val);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.size());
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        for (Integer key : keys) {
            out.writeInt(key);
            out.writeInt(values.get(key));
        }
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        CountGradient agg = (CountGradient) arg0;
        return ComparisonChain.start().
                compare(values.size(), agg.getValues().size()).
                result();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CountGradient))
            return false;
        return (this.compareTo((CountGradient) o) == 0) ? true : false;
    }
    
    @Override
    public void add(Aggregation agg) {
        if (!(agg instanceof CountGradient))
            throw new IllegalArgumentException("Invalid aggregation: expect count gradient, got " +
                    FrameworkUtils.functionToString(agg.getId()));
        CountGradient aggregation = (CountGradient) agg;
        HashMap<Integer, Integer> aggValues = aggregation.getValues();
        Set<Integer> keys = aggValues.keySet();
        for (Integer key : keys) {
            Integer val = aggValues.get(key);
            if (this.values.containsKey(key)) {
                val += this.values.get(key);
            }
            this.values.put(key, val);
        }
    }
}
