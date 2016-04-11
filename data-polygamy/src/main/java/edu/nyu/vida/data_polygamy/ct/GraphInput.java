/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

public interface GraphInput {

	int getMaxDegree();
	int getVertexCount();
	MyIntList getStar(int v);
	float[] getFnVertices();
	boolean isIgnored(int v);
	int getTime(int tid);
}
