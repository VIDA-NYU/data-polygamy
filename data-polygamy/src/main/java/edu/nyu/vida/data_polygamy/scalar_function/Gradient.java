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

public class Gradient extends Aggregation {
    
    private HashMap<Integer, ArrayList<Float>> values =
            new HashMap<Integer, ArrayList<Float>>();
    
    public Gradient() {
        this.id = Function.GRADIENT;
    }
    
    public HashMap<Integer, ArrayList<Float>> getValues() {
        return values;
    }

    private float getAverage(ArrayList<Float> values) {
        float avg = 0;
        for (float val : values) {
            avg += val;
        }
        return avg / (float) values.size();
    }
    
    private float getDirection() {
        ArrayList<Vector> vectorArray = new ArrayList<Vector>(); 
        
        Float lastX = null;
        Float lastY = null;
        float lastRealX = 0;
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        for (Integer key : keys) {
            if (lastX == null) {
                lastX = (float) 0;
                lastRealX = (float) key;
                lastY = getAverage(values.get(key));
                continue;
            }
            float currentX = (float) key - lastRealX;
            float currentY = getAverage(values.get(key));
            Vector vector = new Vector(lastX, lastY, currentX, currentY);
            vectorArray.add(vector);
            lastX = currentX;
            lastY = currentY;
            lastRealX = (float) key;
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
        
        ArrayList<Float> timeVal = new ArrayList<Float>();
        if (values.containsKey(time)) {
            timeVal = values.get(time);
        }
        
        timeVal.add(value);
        values.put(time, timeVal);
    }
    
    @Override
    public void reset() {
        values = new HashMap<Integer, ArrayList<Float>>();
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
        values = new HashMap<Integer, ArrayList<Float>>(size);
        for (int i = 0; i < size; i++) {
            int intSize = in.readInt();
            int time = in.readInt();
            ArrayList<Float> vals = new ArrayList<Float>(intSize);
            float timeVal = 0;
            for (int j = 0; j < intSize; j++) {
                timeVal = in.readFloat();
                vals.add(timeVal);
            }
            values.put(time, vals);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.size());
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        for (Integer key : keys) {
            ArrayList<Float> vals = values.get(key);
            out.writeInt(vals.size());
            out.writeInt(key);
            for (float val : vals) {
                out.writeFloat(val);
            }
        }
    }
    
    @Override
    public int compareTo(Aggregation arg0) {
        Gradient agg = (Gradient) arg0;
        return ComparisonChain.start().
                compare(values.size(), agg.getValues().size()).
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
        HashMap<Integer, ArrayList<Float>> aggValues = aggregation.getValues();
        Set<Integer> keys = aggValues.keySet();
        for (Integer key : keys) {
            ArrayList<Float> vals = aggValues.get(key);
            if (this.values.containsKey(key)) {
                vals.addAll(this.values.get(key));
            }
            this.values.put(key, vals);
        }
    }
}
