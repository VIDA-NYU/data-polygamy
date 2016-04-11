/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import java.io.Serializable;

public class Branch implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public int from;
	public int to;
	
	public MyIntList arcs = new MyIntList(2);
	
	public int parent;
	public MyIntList children = new MyIntList(2);
	
	@Override
	public String toString() {
		return from + " " + to;
	}
}
