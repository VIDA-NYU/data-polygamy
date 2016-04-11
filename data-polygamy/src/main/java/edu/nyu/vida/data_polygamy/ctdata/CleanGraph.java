/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class CleanGraph implements Serializable {

	private static final long serialVersionUID = 1L;

	public class Arc implements Serializable {
		private static final long serialVersionUID = 1L;

		int v1;
		int v2;
		int icol;
		double fn;
		boolean removed;
				
		public boolean equals(Object obj) {
			Arc a = (Arc) obj;
			return (a.icol == icol);
		}
	}
	
	public class Node implements Serializable {
		private static final long serialVersionUID = 1L;

		int v;
		boolean removed;
		ArrayList<Arc> adj = new ArrayList<Arc>();
	}
	
	public Node [] nodes;
	public Arc [] arcs;

	ArrayList<Node> an = new ArrayList<Node>();
	ArrayList<Arc> ar = new ArrayList<Arc>();
	public HashMap<Integer, Integer> vmap = new HashMap<Integer, Integer>();
	int ct = 0;
	short ect = 0;
	float persistence;
	
	public void addNode(int v) {
		Node n = new Node();
		n.v = v;
		an.add(n);
		vmap.put(v, ct);
		ct ++;
	}
	
	public void setup() {
		nodes = an.toArray(new Node[0]);
		arcs = ar.toArray(new Arc[0]);
	}
	
	public void addArc(int v1, int v2, double f) {
		Arc a = new Arc();
		a.v1 = vmap.get(v1);
		a.v2 = vmap.get(v2);
		ect ++;
		a.icol = ect;
		a.fn = f;
		ar.add(a);
		
		an.get(a.v1).adj.add(a);
		an.get(a.v2).adj.add(a);
	}

	public void removeDeg2Nodes() {
		// remove degree 2 vertices
		int ct = 1;
		while(ct != 0) {
			ct = 0;
			for(int i = 0;i < nodes.length;i ++) {
				if(nodes[i].adj.size() == 0) {
					nodes[i].removed = true;
				}
				if(!nodes[i].removed && nodes[i].adj.size() == 1) {
					removeNode(i);
					ct ++;
				}
				if(!nodes[i].removed && nodes[i].adj.size() == 2) {
					mergeNode(i);
					ct ++;
				}
			}
		}
	}

	private void removeNode(int i) {
		Arc e1 = nodes[i].adj.get(0);
		
		nodes[i].removed = true;
		e1.removed = true;
		if(e1.v1 == i) {
			nodes[e1.v2].adj.remove(e1);	
		} else {
			nodes[e1.v1].adj.remove(e1);
		}
	}

	private void mergeNode(int i) {
		Arc e1 = nodes[i].adj.get(0);
		Arc e2 = nodes[i].adj.get(1);
		
		nodes[i].removed = true;
		if(e1.icol > e2.icol) {
			e1.icol = e2.icol;
		}
		e1.fn += e2.fn;
		if(e1.v1 == i) {
			if(e2.v1 == i) {
				e1.v1 = e2.v2;
			} else {
				e1.v1 = e2.v1;
			}
			nodes[e1.v1].adj.remove(e2);
			nodes[e1.v1].adj.add(e1);
		} else {
			if(e2.v1 == i) {
				e1.v2 = e2.v2;
			} else {
				e1.v2 = e2.v1;
			}
			nodes[e1.v2].adj.remove(e2);
			nodes[e1.v2].adj.add(e1);
		}
		e2.removed = true;
	}
	
	public void outputCleanedGraph(String op) {
		try {
			PrintStream p = new PrintStream(op);
			
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
			int ct = 0;
			int []vmap = new int[nodes.length];
			Arrays.fill(vmap, -1);
			for(int i = 0;i < nodes.length;i ++) {
				if(!nodes[i].removed) {
					vmap[i] = ct ++;
					p.println(x[i] + " " + y[i]);
				}
			}
			
			for(int i = 0;i < arcs.length;i ++) {
				if(!arcs[i].removed) {
					int v1 = nodes[arcs[i].v1].v;
					int v2 = nodes[arcs[i].v2].v;
					if(vmap[v1] == -1 || vmap[v2] == -1) {
						Utilities.er("!!!!!!!!!!!!!!!!!!!!!!!");
					}
					p.println(vmap[v1] + " " + vmap[v2] + " " + arcs[i].fn);
				}
			}
			
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void outputVertices(String op) {
		try {
			PrintStream p = new PrintStream(op);
			int ct = 0;
			for(int i = 0;i < nodes.length;i ++) {
				
				if(!nodes[i].removed) {
					p.println(ct + " 3");
					ct ++;
				}
			}
			
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	double [] x,y, wts;
	
	private void load(String file) {
		try {
			BufferedReader buf = new BufferedReader(new FileReader(file));
			String l = buf.readLine();
			String [] s = Utilities.splitString(l);
			int nv = Integer.parseInt(s[0].trim());
			int ne = Integer.parseInt(s[1].trim());
			
			x = new double[nv];
			y = new double[nv];
			wts = new double[ne];
			for(int i = 0;i < nv;i ++) {
				l = buf.readLine();
				s = Utilities.splitString(l);
				x[i] = Double.parseDouble(s[0]);
				y[i] = Double.parseDouble(s[1]);
				addNode(i);
			}
			for(int i = 0;i < ne;i ++) {
				l = buf.readLine();
				s = Utilities.splitString(l);
				int v1 = Integer.parseInt(s[0].trim());
				int v2 = Integer.parseInt(s[1].trim());
				wts[i] = Double.parseDouble(s[2]);
				addArc(v1,v2,wts[i]);
			}
			buf.close();
			setup();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String [] args) {
		CleanGraph cg = new CleanGraph();
		cg.load("old/taxi/filtered-graph.txt");
		cg.removeDeg2Nodes();
		cg.outputCleanedGraph("scalar/cleanFilteredGraph.txt");
	}
}
