/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class TopologyGraph implements Serializable {
    private static final long serialVersionUID = 1L;

	class Vertex implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		int in;
		float per;
		HashSet<Integer> vertices = new HashSet<Integer>();
		byte type;
	}
	
	class Edge implements Serializable {
	    private static final long serialVersionUID = 1L;
		
		int v1, v2;
		int hc;
		
		public Edge(int v1, int v2) {
			this.v1 = v1;
			this.v2 = v2;
			hc = ("" + v1 + " " + v2).hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			if(!(obj instanceof Edge)) {
				return false;
			}
			Edge e = (Edge) obj;
			return (e.v1 == v1 && e.v2 == v2);
		}
		
		@Override
		public int hashCode() {
			return hc; 
		}
	}
	
	class Partition implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		ArrayList<Vertex> vertices = new ArrayList<Vertex>();
	}
	
	Partition [] parts = new Partition[2];
	HashSet<Edge> edges = new HashSet<Edge>();
	HashMap<Edge, Integer> edgeMap = new HashMap<Edge, Integer>();
	long domainSize = 0;
	
	public void loadPartition(int i, String per) throws IOException {
		parts[i] = new Partition();
		
		BufferedReader buf = new BufferedReader(new FileReader(per));
		String [] s = Utilities.getLine(buf);
		int ct = 0;
		while(s != null) {
			Vertex v = new Vertex();
			v.in = ct ++;
			v.per = Float.parseFloat(s[1].trim());
			v.type = Byte.parseByte(s[5].trim());
			parts[i].vertices.add(v);
			s = Utilities.getLine(buf);
		}
		buf.close();
	}
	
	public void computeEdges(String part1, String part2) throws IOException {
		BufferedReader buf1 = new BufferedReader(new FileReader(part1));
		BufferedReader buf2 = new BufferedReader(new FileReader(part2));
		String [] s1 = Utilities.getLine(buf1);
		String [] s2 = Utilities.getLine(buf2);
		
		int nv = Integer.parseInt(s1[0].trim());
		domainSize = nv;
		for(int i = 0;i < nv;i ++) {
			s1 = Utilities.getLine(buf1);
			s2 = Utilities.getLine(buf2);
			int r1 = Integer.parseInt(s1[0].trim());
			int r2 = Integer.parseInt(s2[0].trim());
			
			if(r1 != -1) {
				parts[0].vertices.get(r1).vertices.add(i);
			}
			if(r2 != -1) {
				parts[1].vertices.get(r2).vertices.add(i);
			}
			if(r1 == -1 || r2 == -1) {
				continue;
			}
			
			byte type1 = parts[0].vertices.get(r1).type;
			byte type2 = parts[1].vertices.get(r2).type;
			if((type1 & type2) == 0) {
				continue;
			}
			
			Edge ed = new Edge(r1,r2);
			Integer ct = edgeMap.get(ed);
			if(ct == null) {
				ct = 0;
			}
			ct ++;
			edgeMap.put(ed, ct);
			edges.add(ed);
		}
		buf1.close();
		buf2.close();
		
	}

	public void writeGraph(String graph) throws FileNotFoundException {
		PrintStream pr = new PrintStream(graph);

		pr.println(parts[0].vertices.size() + " " + parts[1].vertices.size()
				+ " " + edges.size());
		for (Edge e : edges) {
			double per1 = parts[0].vertices.get(e.v1).per;
			double per2 = parts[1].vertices.get(e.v2).per;
			double maxp = Math.max(per1, per2);
			maxp = (maxp == 0)?1:maxp;
			double wt = Math.min(per1, per2) / maxp;

			int intersection = edgeMap.get(e);
			double dist = (double) intersection;// / domainSize;
			wt *= dist;
			
			pr.println(e.v1 + " " + e.v2 + " " + wt);
		}
		pr.close();
	}
}