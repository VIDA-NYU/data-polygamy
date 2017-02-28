/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import edu.nyu.vida.data_polygamy.ct.GraphInput;
import edu.nyu.vida.data_polygamy.ct.MyIntList;
import edu.nyu.vida.data_polygamy.ctdata.GraphFunctions.Edge;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class TimeSeries2DFunction implements GraphInput, Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public float [] fnVertices;
	public int nv;
	IntOpenHashSet [] nodes;
	public boolean[] ignore;
	public int nt;
	
	int startTime;
	int tempRes;
	DateTime start;
	
	public TimeSeries2DFunction(ArrayList<SpatioTemporalVal> data, IntOpenHashSet nodeSet,
	        int[][] edges2D, int nv, int tempRes, int stTime, int enTime) {
	    this.nv = nv;
	    
	    int timeSteps = FrameworkUtils.getTimeSteps(tempRes, stTime, enTime);
	    startTime = stTime;
	    this.tempRes = tempRes;
	    start = new DateTime(((long)stTime)*1000, DateTimeZone.UTC);
	    
		loadGraph(timeSteps, nodeSet, edges2D);
		loadFunction(data);
	}
	
	public void loadFunction(ArrayList<SpatioTemporalVal> data) {
		for (SpatioTemporalVal val : data) {
			int time = val.getTemporal();
			int j = FrameworkUtils.getTimeSteps(tempRes, startTime, time) - 1;
			if(j < 0 || j >= nt) {
				Utilities.er("Invalid time step. cannot happen");
			}
			int in = j * nv + val.getSpatial();
			fnVertices[in] = val.getVal();
		}
	}

	
	public void loadGraph(int timeSteps, IntOpenHashSet nodeSet,
	        int[][] edges2D) {
		try {
			ignore = new boolean [nv];
			Arrays.fill(ignore, true);
			for(int v: nodeSet) {
				ignore[v] = false;
			}
			GraphFunctions gf = new GraphFunctions(edges2D,nv);
            ArrayList<Edge> edges = gf.updateIgnoreSet(nodeSet);
			
			nodes = new IntOpenHashSet[nv];
			int nt = timeSteps;
			this.nt = nt;
			fnVertices = new float[nv * nt];
			
			for(int i = 0;i < nv;i ++) {
				// Ignoring coordinates. not required
				nodes[i] = new IntOpenHashSet();
			}
			for(int i = 0; i < edges2D.length; i++) {
				int v1 = edges2D[i][0];
				int v2 = edges2D[i][1];
				if(ignore[v1] || ignore[v2]) {
					continue;
				}
				nodes[v1].add(v2);
				nodes[v2].add(v1);
			}
            for (Edge e : edges) {
                nodes[e.v1].add(e.v2);
                nodes[e.v2].add(e.v1);
            }
			init();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	int maxDegree;
	private void init() {
		maxDegree = -1;
		for(int i = 0;i < nodes.length;i ++) {
			maxDegree = Math.max(maxDegree, nodes[i].size());
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
		
		if(ignore[vv]) {
			return list;
		}
		if(time - 1 >= 0) {
			int tv = v - nv;
			list.add(tv);
		}
		for(Iterator<Integer> it = nodes[vv].iterator();it.hasNext();) {
			int av = it.next();
			int tv = nv * time + av;
			list.add(tv);
		}
		if(time + 1 < nt) {
			int tv = v + nv;
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
		int vv = v % nv;
		return ignore[vv];
	}

	public void writeIndividualFns(String opFile) throws FileNotFoundException {
		int ct = 0;
		for(int i = 0;i < nt;i ++) {
			PrintStream pr = new PrintStream(opFile + "-" + i + ".txt");
			for(int j = 0;j < nv;j ++) {
				pr.println(fnVertices[ct]);
				ct ++;
			}
			pr.close();
		}
	}

	@Override
	public int getTime(int i) {
		return FrameworkUtils.addTimeSteps(tempRes, i, start);
	}

}
