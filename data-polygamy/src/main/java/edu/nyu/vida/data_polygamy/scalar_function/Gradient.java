/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.scalar_function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ComparisonChain;

import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Function;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.Vector;

public class Gradient extends Aggregation {
    
    public class Pair {
        private float val = 0;
        private int count = 0;
        
        public Pair() {}
        
        public Pair(float val, int count) {
            this.val = val;
            this.count = count;
        }
        
        public void addVal(float val) {
            this.val += val;
            count++;
        }
        
        public void addVal(float val, int count) {
            this.val += val;
            this.count += count;
        }
        
        public float getVal() {
            return this.val;
        }
        
        public int getCount() {
            return this.count;
        }
        
        public float getAvg() {
            return this.val / this.count;
        }
    }
    
    private HashMap<Integer, Pair> values =
            new HashMap<Integer, Pair>();
    
    public Gradient() {
        this.id = Function.GRADIENT;
    }
    
    public HashMap<Integer, Pair> getValues() {
        return values;
    }
    
    private float getDirection() {
        Vector[] vectorArray = new Vector[values.size()-1]; 
        
        Float lastX = null;
        Float lastY = null;
        float lastRealX = 0;
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        int id = 0;
        for (Integer key : keys) {
            if (lastX == null) {
                lastX = (float) 0;
                lastRealX = (float) key;
                lastY = values.get(key).getAvg();
                continue;
            }
            float currentX = (float) key - lastRealX;
            float currentY = values.get(key).getAvg();
            Vector vector = new Vector(lastX, lastY, currentX, currentY);
            vectorArray[id] = vector;
            lastX = currentX;
            lastY = currentY;
            lastRealX = (float) key;
            id++;
        }
        
        float x1 = 0;
        float x2 = 0;
        float y1 = 0;
        float y2 = 0;
        int size = vectorArray.length;
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
        
        Pair timeVal = new Pair();
        if (values.containsKey(time)) {
            timeVal = values.get(time);
        }

        timeVal.addVal(value);
        values.put(time, timeVal);
    }
    
    @Override
    public void reset() {
        values = new HashMap<Integer, Pair>();
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
        values = new HashMap<Integer, Pair>(size);
        for (int i = 0; i < size; i++) {
            int time = in.readInt();
            float val = in.readFloat();
            int count = in.readInt();
            values.put(time, new Pair(val, count));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.size());
        SortedSet<Integer> keys = new TreeSet<Integer>(values.keySet());
        for (Integer key : keys) {
            Pair val = values.get(key);
            out.writeInt(key);
            out.writeFloat(val.getVal());
            out.writeInt(val.getCount());
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
        HashMap<Integer, Pair> aggValues = aggregation.getValues();
        Set<Integer> keys = aggValues.keySet();
        for (Integer key : keys) {
            Pair val = aggValues.get(key);
            if (this.values.containsKey(key)) {
                Pair otherVal = this.values.get(key);
                val.addVal(otherVal.getVal(), otherVal.getCount());
            }
            this.values.put(key, val);
        }
    }
}
