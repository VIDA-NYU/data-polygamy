/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class CleanReebGraph implements Serializable {

	private static final long serialVersionUID = 1L;

	public static enum VertexType {
		MINIMA, MAXIMA, SADDLE, NONE
	}
	
	public class Arc implements Comparable<Arc>, Serializable {
		private static final long serialVersionUID = 1L;

		int v1;
		int v2;
		float fn;
		int icol;
		boolean removed;
		HashSet<Integer> segment = new HashSet<Integer>();
		
		public boolean equals(Object obj) {
			Arc a = (Arc) obj;
			return (a.icol == icol);
		}

		public int compareTo(Arc o) {
			float ff = fn - o.fn;
			if(ff < 0)
				return -1;
			else if(ff > 0)
				return 1;
			return 0;
		}
	}
	
	public class Node implements Serializable {
		private static final long serialVersionUID = 1L;

		int v;
		boolean removed;
		float fn;
		VertexType type;
		ArrayList<Arc> prev = new ArrayList<Arc>();
		ArrayList<Arc> next = new ArrayList<Arc>();
	}
	
	public Node [] nodes;
	public Arc [] arcs;

	ArrayList<Node> an = new ArrayList<Node>();
	ArrayList<Arc> ar = new ArrayList<Arc>();
	public HashMap<Integer, Integer> vmap = new HashMap<Integer, Integer>();
	int ct = 0;
	short ect = 0;
	float min = Float.MAX_VALUE;
	float max = -Float.MAX_VALUE;
	float persistence;
	
	public void addNode(int v, float fn, byte type) {
		Node n = new Node();
		n.fn = fn;
		n.v = v;
		if(type == CTAlgorithm.MINIMUM) {
			n.type = VertexType.MINIMA;
		}
		if(type == CTAlgorithm.MAXIMUM) {
			n.type = VertexType.MAXIMA;
		}
		if(type == CTAlgorithm.SADDLE) {
			n.type = VertexType.SADDLE;
		}
		an.add(n);
		vmap.put(v, ct);
		ct ++;
		
		max = Math.max(max, fn);
		min = Math.min(min, fn);
	}
	
	public void setup() {
		nodes = an.toArray(new Node[0]);
		arcs = ar.toArray(new Arc[0]);
		persistence = max - min;
	}
	
	public void addArc(int v1, int v2) {
		Arc a = new Arc();
		a.v1 = vmap.get(v1);
		a.v2 = vmap.get(v2);
		a.fn = an.get(a.v2).fn - an.get(a.v1).fn;

		a.segment.add(v1);
		a.segment.add(v2);
		ect ++;
		a.icol = ect;
		ar.add(a);
		
		an.get(a.v1).next.add(a);
		an.get(a.v2).prev.add(a);
	}

	public void removeDeg2Nodes() {
		// remove degree 2 vertices
		for(int i = 0;i < nodes.length;i ++) {
			if(nodes[i].prev.size() == 0 && nodes[i].next.size() == 0) {
				nodes[i].removed = true;
			}
			if(!nodes[i].removed && nodes[i].next.size() == 1 && nodes[i].prev.size() == 1) {
				mergeNode(i);
			}
		}
	}
	
	private void mergeNode(int i) {
		Arc e1 = nodes[i].prev.get(0);
		Arc e2 = nodes[i].next.get(0);
		
		nodes[i].removed = true;
		e1.v2 = e2.v2;
		e2.removed = true;
		e1.segment.addAll(e2.segment);
		if(e1.icol > e2.icol) {
			e1.icol = e2.icol;
		}
		e1.fn += e2.fn;
		nodes[e1.v2].prev.remove(e2);
		nodes[e1.v2].prev.add(e1);
	}
	
	public void outputSegmentation(PrintStream p, int noVerts) {
		int [] part = new int [nodes.length];
		Arrays.fill(part, -1);
		for(int i = 0;i < arcs.length;i ++) {
			if(!arcs[i].removed) {
				for(Iterator<Integer> it = arcs[i].segment.iterator();it.hasNext();) {
					int v = it.next();
					if(v < noVerts) {
						part[v] = arcs[i].icol;
					}
				}
			}
		}
		p.println(noVerts);
		for(int i = 0;i < noVerts;i ++) {
			if(part[i] == -1) {
				Utilities.er("Vertex not part of arc!!");
			}
			p.println(part[i]);
		}
	}
	
	public void outputReebGraph(PrintStream p) {
		int nv = 0;
		int ne = 0;
		for(int i = 0;i < nodes.length;i ++) {
			if(!nodes[i].removed) {
				nv ++;
			}
		}
		
		for(int i = 0;i < arcs.length;i ++) {
			if(!arcs[i].removed) {
				arcs[i].icol = ne;
				ne ++;
			}
		}

		p.println(nv + " " + ne);
		System.out.println(nv + " " + ne);
		for(int i = 0;i < nodes.length;i ++) {
			if(!nodes[i].removed) {
				p.println(nodes[i].v + " " + nodes[i].fn + " " + nodes[i].type);
			}
		}
		
		for(int i = 0;i < arcs.length;i ++) {
			if(!arcs[i].removed) {
				p.print(nodes[arcs[i].v1].v + " " + nodes[arcs[i].v2].v + " ");
				p.println();
			}
		}
	}
	
}
