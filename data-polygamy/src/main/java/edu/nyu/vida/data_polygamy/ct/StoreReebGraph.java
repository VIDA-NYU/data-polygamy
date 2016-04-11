/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import java.io.PrintStream;
import java.io.Serializable;


public class StoreReebGraph implements Serializable {

	private static final long serialVersionUID = 1L;

	int [] nodes;
	byte [] nodeType;
	float [] nodeFn;
	MyIntList xarc;
	MyIntList yarc;

	public StoreReebGraph(int no) {
		nodes = new int[no];
		nodeType = new byte[no];
		nodeFn = new float[no];
		xarc = new MyIntList((int) (no*1.5));
		yarc = new MyIntList((int) (no*1.5));
	}
	
	int curNode = 0;
	public void addNode(int v, float fn, byte type) {
		nodes[curNode] = v;
		nodeType[curNode] = type;
		nodeFn[curNode] = fn;
		curNode ++;
	}
	
	CleanReebGraph rg;
	public void setup() {
		 rg = new CleanReebGraph();
		 System.out.println("No. of Nodes: " + curNode);
		 for(int i = 0;i < curNode;i ++) {
			 rg.addNode(nodes[i], nodeFn[i], nodeType[i]);
		 }
		 
		 for(int i = 0;i < xarc.length;i ++) {
			 rg.addArc(xarc.array[i], yarc.array[i]);
		 }
		 rg.setup();
	}
	
	public void addArc(int v1, int v2) {
		xarc.add(v1);
		yarc.add(v2);
	}

	public void removeDeg2Nodes() {
		rg.removeDeg2Nodes();
	}
	
	
	public void outputReebGraph(PrintStream p) {
		rg.outputReebGraph(p);
	}
	
	public void outputReebGraph(PrintStream p, PrintStream part, int noVerts) {
		rg.outputReebGraph(p);
		rg.outputSegmentation(part, noVerts);
	}
}
