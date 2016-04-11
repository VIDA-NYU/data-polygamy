/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

public interface Function {
	
	public void init(float [] fn, Branch [] br);
	public void update(Branch [] br, int brNo);
	public void branchRemoved(Branch [] br, int brNo, boolean[] invalid);
}
