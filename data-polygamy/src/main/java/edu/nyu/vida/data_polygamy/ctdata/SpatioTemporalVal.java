/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import java.io.Serializable;

import com.google.common.base.Objects;

public class SpatioTemporalVal implements Comparable<SpatioTemporalVal>, Serializable {
    private static final long serialVersionUID = 1L;
    
    private int spatial;
    private int temporal;
	private float val;
	
	public SpatioTemporalVal(int spatial, int temporal, float val) {
	    this.spatial = spatial;
	    this.temporal = temporal;
	    this.val = val;
	}
	
	public int getSpatial() {
        return spatial;
    }
	
	public int getTemporal() {
        return temporal;
    }
	
	public float getVal() {
        return val;
    }
	
	public void setSpatial(int spatial) {
        this.spatial = spatial;
    }

    public void setTemporal(int temporal) {
        this.temporal = temporal;
    }

    public void setVal(float val) {
        this.val = val;
    }
	
	@Override
	public int compareTo(SpatioTemporalVal t) {
		return temporal - t.getTemporal();
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(this.temporal, this.spatial);
	}
	
	@Override
	public boolean equals(Object obj) {
		return (this.temporal == ((SpatioTemporalVal)obj).getTemporal() &&
		        this.spatial == ((SpatioTemporalVal)obj).getSpatial());
	}
    
}