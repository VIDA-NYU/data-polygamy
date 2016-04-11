/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import edu.nyu.vida.data_polygamy.utils.Utilities;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;

import edu.nyu.vida.data_polygamy.utils.DisjointSets;

public class GraphFunctions {

	public static class Edge {
		public int v1,v2;
	}
	int nv;
	IntOpenHashSet [] nodes;
	
	public GraphFunctions(String graph) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(graph));
			String [] s = Utilities.splitString(reader.readLine().trim());
			nv = Integer.parseInt(s[0].trim());
			
			nodes = new IntOpenHashSet[nv];
			int ne = Integer.parseInt(s[1].trim());
			
			for(int i = 0;i < nv;i ++) {
				nodes[i] = new IntOpenHashSet();
			}
			for(int i = 0;i < ne;i ++) {
				s = Utilities.splitString(reader.readLine().trim());
				int v1 = Integer.parseInt(s[0].trim());
				int v2 = Integer.parseInt(s[1].trim());
				if(v1 == v2) {
					continue;
				}
				nodes[v1].add(v2);
				nodes[v2].add(v1);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public GraphFunctions(ArrayList<Integer[]> edges2D, int noNodes) {
	    try {
            nv = noNodes;
            nodes = new IntOpenHashSet[nv];
            
            for(int i = 0;i < nv;i ++) {
                nodes[i] = new IntOpenHashSet();
            }
            for(Integer[] edge: edges2D) {
                int v1 = edge[0];
                int v2 = edge[1];
                nodes[v1].add(v2);
                nodes[v2].add(v1);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

	public ArrayList<Edge> updateIgnoreSet(IntOpenHashSet nodeSet) {
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		IntOpenHashSet [] n = new IntOpenHashSet[nv];
		for(int i = 0;i < nv;i ++) {
			if(nodeSet.contains(i)) {
				n[i] = new IntOpenHashSet();
				for(int v: nodes[i]) {
					if(nodeSet.contains(v)) {
						n[i].add(v);
					}
				}
			}
		}
		
		DisjointSets dj = new DisjointSets();
		for(int i = 0;i < nv;i ++) {
			if(n[i] != null) {
				for(int v: n[i]) {
					dj.union(dj.find(v), dj.find(i));
				}
			}
		}		
		Int2ObjectOpenHashMap<IntOpenHashSet> map = new Int2ObjectOpenHashMap<IntOpenHashSet>();
		for(int i = 0;i < nv;i ++) {
			if(n[i] != null) {
				int c = dj.find(i);
				if(map.get(c) == null) {
					map.put(c, new IntOpenHashSet());
				}
				map.get(c).add(i);
			}
		}
		
		boolean connected = (map.size() == 1);
		ArrayList<Integer> queue = new ArrayList<Integer>();
		IntOpenHashSet reached = new IntOpenHashSet();
		int [] prev = new int[nv];
		while(!connected) {
			IntOpenHashSet root = map.values().iterator().next();
			int st = root.iterator().next();
			int in = -1;
			queue.clear();
			reached.clear();
			queue.add(st);
			reached.add(st);
			Arrays.fill(prev,-1); 
			while(!queue.isEmpty()) {
				st = queue.remove(0);
				
				if(!root.contains(st) && n[st] != null) {
					break;
				}
				for(int v: nodes[st]) {
					if(!reached.contains(v)) {
						queue.add(v);
						reached.add(v);
						prev[v] = st;
					}
				}
				if(root.contains(st)) {
					in = st;
				}
				st = -1;
			}
			if(st == -1) {
				Utilities.er("The original graph is not connected????");
			}
			
			// connect components containing st, with root.
			int c1 = dj.find(in);
			int c2 = dj.find(st);
			IntOpenHashSet merge = map.remove(c1);
			merge.addAll(map.remove(c2));
			dj.union(dj.find(st), dj.find(in));
			c1 = dj.find(in);
			if(map.containsKey(c1)) {
				Utilities.er("not possible!!!!");
			}
			map.put(c1, merge);
			Edge e = new Edge();
			e.v1 = in;
			e.v2 = st;
			
			nodes[in].add(st);
			nodes[st].add(in);
			edges.add(e);
			
			connected = (map.size() == 1);
		}
		return edges;
	}

	
	void countComponents(IntOpenHashSet nodeSet) {
		IntOpenHashSet [] n = new IntOpenHashSet[nv];
		for(int i = 0;i < nv;i ++) {
			if(nodeSet.contains(i)) {
				n[i] = new IntOpenHashSet();
				for(int v: nodes[i]) {
					if(nodeSet.contains(v)) {
						n[i].add(v);
					}
				}
			}
		}
		
		DisjointSets dj = new DisjointSets();
		for(int i = 0;i < nv;i ++) {
			if(n[i] != null) {
				for(int v: n[i]) {
					dj.union(dj.find(v), dj.find(i));
				}
			}
		}		
		Int2ObjectOpenHashMap< IntOpenHashSet > map = new Int2ObjectOpenHashMap<IntOpenHashSet>();
		for(int i = 0;i < nv;i ++) {
			if(n[i] != null) {
				int c = dj.find(i);
				if(map.get(c) == null) {
					map.put(c, new IntOpenHashSet());
				}
				map.get(c).add(i);
			}
		}
		
		System.out.println("no. of components: " + map.size());
	}

}
