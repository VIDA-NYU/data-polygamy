/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import edu.nyu.vida.data_polygamy.ct.GraphInput;
import edu.nyu.vida.data_polygamy.ct.MyIntList;

public class TimeSeriesFunction implements GraphInput, Serializable {
	
    private static final long serialVersionUID = 1L;

    public class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public HashSet<Integer> adjacencies = new HashSet<Integer>();
		public double x,y;
		public double [] c;
	}
	
	public int nv;
	int maxDegree;
	public Node [] nodes;
	public String [] coordNames;
	public double [] min,max;

	public float [] fnVertices;
	public int [] time;
	
	public TimeSeriesFunction(ArrayList<SpatioTemporalVal> data) {
		this.loadData(data);
	}
	
	public void copyTo(TimeSeriesFunction target) {
		target.nodes = nodes;
		target.nv = nv;
		target.maxDegree = maxDegree;
		target.max = max;
		target.min = min;
		target.coordNames = coordNames;
		target.fnVertices = new float[nv];
	}
	
	public void loadGraph(int no) {
		nv = no;
		
		nodes = new Node[nv];
		fnVertices = new float[nv];
		time = new int[nv];
		for(int i = 0;i < nv;i ++) {
			// Ignoring coordinates. not required
			nodes[i] = new Node();
		}
		for(int i = 0;i < nv - 1;i ++) {
			int v1 = i;
			int v2 = i + 1;
			nodes[v1].adjacencies.add(v2);
			nodes[v2].adjacencies.add(v1);
		}
		init();

	}
	
	public void loadData(ArrayList<SpatioTemporalVal> data) {
		loadGraph(data.size());
		readFunction(data);
	}

	public void readFunction(ArrayList<SpatioTemporalVal> data) {
		for (int i = 0; i < nv; i++) {
			fnVertices[i] = data.get(i).getVal();
			time[i] = data.get(i).getTemporal();
		}
	}

	private void init() {
		maxDegree = -1;
		for(int i = 0;i < nodes.length;i ++) {
			maxDegree = Math.max(maxDegree, nodes[i].adjacencies.size());
		}
	}

	@Override
	public int getMaxDegree() {
		return maxDegree;
	}

	@Override
	public int getVertexCount() {
		return fnVertices.length;
	}

	MyIntList list = new MyIntList();
	@Override
	public MyIntList getStar(int v) {
		int time = v / nv;
		int vv = v % nv;
		list.clear();
		
		for(Iterator<Integer> it = nodes[vv].adjacencies.iterator();it.hasNext();) {
			int av = it.next();
			int tv = nv * time + av;
			list.add(tv);
		}
		return list;
	}

	@Override
	public float[] getFnVertices() {
		return fnVertices;
	}

	@Override
	public boolean isIgnored(int v) {
		return false;
	}

	public void difference(TimeSeriesFunction data2) {
		for(int i = 0;i < fnVertices.length;i ++) {
			fnVertices[i] = Math.abs(fnVertices[i] - data2.fnVertices[i]);
		}
	}
	
	public void minus(TimeSeriesFunction data2) {
		for(int i = 0;i < fnVertices.length;i ++) {
			fnVertices[i] = fnVertices[i] - data2.fnVertices[i];
		}
	}

	@Override
	public int getTime(int tid) {
		return time[tid];
	}
	
}
