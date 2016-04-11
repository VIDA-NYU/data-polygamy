/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import edu.nyu.vida.data_polygamy.utils.DisjointSets;
import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;


public class CTAlgorithm implements Serializable {
    private static final long serialVersionUID = 1L;
    
	public static enum TreeType {ContourTree, SplitTree, JoinTree};	
	GraphInput data;
	StoreReebGraph rg;
	
	int [] cpMap;
	DisjointSets nodes;
	ContourTree ct;
	MyArrays myArrays = new MyArrays();
	
	public void computeTree(GraphInput data, TreeType type) throws IOException {
		this.data = data;
		
		long ct = System.nanoTime();
		setupData();
		orderVertices();
		switch(type) {
		case ContourTree:
			computeContourTree();
			break;
			
		case SplitTree:
			computeSplitTree();
			break;
			
		case JoinTree:
			computeJoinTree();
			break;
			
		default:
			Utilities.er("Invalid tree type");	
		}
		
		long en = System.nanoTime();
		ct = (en - ct) / 1000000;
		
		System.out.println("Time taken to compute Reeb Graph : " + ct + " ms");
	}
	
	private void computeSplitTree() {
		findSplitTree();
		rg = new StoreReebGraph(noVertices);
		nodes.clear();
		for(int i = 0;i < sv.length;i ++) {
			int v = sv[i];
			MyIntList star = data.getStar(v);
			int ct = 0;
			for(int x = 0;x < star.length; x++) {
				int tin = star.array[x];
				if(compare(v,tin) > 0) {
					// lowerLink
					ct ++;
				}
			}
			if(ct == 0) {
				// Minimum
				criticalPts[v] = MINIMUM;
			}
			rg.addNode(v, fnVertices[v], criticalPts[v]);
		}
		for(int i = 0;i < sv.length - 1;i ++) {
			int from = sv[i];
			int to = sv[i + 1];
			ct.addJoinArc(from, to);
		}
		ct.mergeTrees(rg);
	}
	
	private void computeJoinTree() {
		for(int i = noVertices - 1;i >= 0; i --) {
			int v = sv[i];
			criticalPts[v] = SADDLE;
			
			MyIntList star = data.getStar(v);
			int ct = 0;
			for(int x = 0;x < star.length; x++) {
				int tin = star.array[x];
				if(compare(v,tin) < 0) {
					// upperLink
					ct ++;
				}
			}
			if(ct == 0) {
				criticalPts[v] = MAXIMUM;
			}
		}
		for(int i = noVertices - 1;i >= 1; i --) {
			int to = sv[i];
			int from = sv[i - 1];
			ct.addSplitArc(from, to);
		}		
		findJoinTree();
		ct.mergeTrees(rg);
	}
	
	private void computeContourTree() {
		findSplitTree();
		findJoinTree();
		ct.mergeTrees(rg);
	}
	
	int noVertices;
	float [] fnVertices;
	void setupData() {		
		maxStar = data.getMaxDegree();
		noVertices = data.getVertexCount();
		fnVertices = data.getFnVertices();
		ct = new ContourTree(noVertices, maxStar, noVertices);
		
		criticalPts = new byte[noVertices];
		
		sv = new int[noVertices];

		for(int i = 0;i < noVertices;i ++) {
			sv[i] = i;
		}
		
		cpMap = new int[noVertices + 1];
		nodes = new DisjointSets();
	}

	public void output(String op) {
		try {
			PrintStream p = new PrintStream(op);
			rg.setup();
			rg.removeDeg2Nodes();
			rg.outputReebGraph(p);
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void output(String op, String part) {
		try {
			PrintStream p = new PrintStream(op);
			PrintStream pt = new PrintStream(part);
			rg.setup();
			rg.removeDeg2Nodes();
			rg.outputReebGraph(p, pt, noVertices);
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	int [] sv;
	int maxStar = 0;

	private void orderVertices() {
		myArrays.sort(sv);
	}

	public static final byte REGULAR = 0;
	public static final byte MINIMUM = 1;
	public static final byte SADDLE = 4;
	public static final byte MAXIMUM = 2;
	
	byte [] criticalPts;
	
	/* Split Tree */
	public void findSplitTree() {
		for(int i = noVertices - 1;i >= 0; i --) {
			int v = sv[i];
			criticalPts[v] = SADDLE;
			processVertex(v);
		}
	}
	
	HashSet<Integer> set = new HashSet<Integer>();
	
	void processVertex(int v) {
		if(v == 24) {
			v *= 1;
		}
		MyIntList star = data.getStar(v);
		set.clear();
		for(int x = 0;x < star.length; x++) {
			int tin = star.array[x];
			if(compare(v,tin) < 0) {
				// upperLink
				int comp = nodes.find(tin);
				set.add(comp);
			}
		}
		if(set.size() == 0) {
			// Maximum
			int comp = nodes.find(v);
			cpMap[comp] = v;
			criticalPts[v] = MAXIMUM;
		} else {
			for(Iterator<Integer> it = set.iterator();it.hasNext();) {
				int comp = it.next();
				int to = cpMap[comp];
				int from = v;
				ct.addSplitArc(from, to);
				nodes.union(nodes.find(comp), nodes.find(v));
			}
			int comp = nodes.find(v);
			cpMap[comp] = v;
		}
	}
	
	/* Join Tree */
	public void findJoinTree() {
		rg = new StoreReebGraph(noVertices);
		nodes.clear();
		for(int i = 0;i < sv.length;i ++) {
			int v = sv[i];
			processVertexJ(v);
		}
	}
	
	void processVertexJ(int v) {
		if(v == 24) {
			v *= 1;
		}
		MyIntList star = data.getStar(v);
		set.clear();
		for(int x = 0;x < star.length; x++) {
			int tin = star.array[x];
			if(compare(v,tin) > 0) {
				// lowerLink
				int comp = nodes.find(tin);
				set.add(comp);
			}
		}
		if(set.size() == 0) {
			// Minimum
			int comp = nodes.find(v);
			cpMap[comp] = v;
			criticalPts[v] = MINIMUM;
//			System.out.println(v);
		} else {
			for(Iterator<Integer> it = set.iterator();it.hasNext();) {
				int comp = it.next();
				int from = cpMap[comp];
				int to = v;
				ct.addJoinArc(from, to);
				nodes.union(nodes.find(comp), nodes.find(v));
			}
			int comp = nodes.find(v);
			cpMap[comp] = v;
		}
		rg.addNode(v, (float)fnVertices[v], criticalPts[v]);
	}
	
	public class MyArrays implements Serializable {
        private static final long serialVersionUID = 1L;

		private static final int INSERTIONSORT_THRESHOLD = 7;

		public void sort(int [] a) {
			int [] aux = clone(a);
			mergeSort(aux, a, 0, a.length, 0);
		}
		
		private int [] clone(int [] a) {
			int[] aux = new int[a.length];
			for(int i = 0;i < a.length;i ++) {
				aux[i] = a[i];
			}
			return aux;
		}
		private void mergeSort(int[] src, int[] dest, int low, int high, int off) {
			int length = high - low;

			// Insertion sort on smallest arrays
			if (length < INSERTIONSORT_THRESHOLD) {
				for (int i = low; i < high; i++)
					for (int j = i; j > low && compare(dest[j - 1], dest[j]) > 0; j--)
						swap(dest, j, j - 1);
				return;
			}

			// Recursively sort halves of dest into src
			int destLow = low;
			int destHigh = high;
			low += off;
			high += off;
			int mid = (low + high) >>> 1;
			mergeSort(dest, src, low, mid, -off);
			mergeSort(dest, src, mid, high, -off);

			// If list is already sorted, just copy from src to dest. This is an
			// optimization that results in faster sorts for nearly ordered lists.
			if (compare(src[mid - 1], src[mid]) <= 0) {
				System.arraycopy(src, low, dest, destLow, length);
				return;
			}

			// Merge sorted halves (now in src) into dest
			for (int i = destLow, p = low, q = mid; i < destHigh; i++) {
				if (q >= high || p < mid && compare(src[p], src[q]) <= 0)
					dest[i] = src[p++];
				else
					dest[i] = src[q++];
			}
		}

		private void swap(int[] x, int a, int b) {
			int t = x[a];
			x[a] = x[b];
			x[b] = t;
		}
	}

	public int compare(int o1, int o2) {
		if(fnVertices[o1] < fnVertices[o2] || (fnVertices[o1] == fnVertices[o2] && o1 < o2)) {
			return -1;
		}
		return 1;
	}
}
