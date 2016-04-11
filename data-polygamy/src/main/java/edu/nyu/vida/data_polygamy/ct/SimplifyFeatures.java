/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import edu.nyu.vida.data_polygamy.ct.ReebGraphData.Arc;
import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class SimplifyFeatures implements Serializable {
    private static final long serialVersionUID = 1L;
	
	public class Feature implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		public int v;
		public int br;
		public MyIntList arcs = new MyIntList();
		public float wt;
		public float exFn;
		public float avgFn;
		public float sadFn;
		
		public byte type;
	}
	
	public Feature [] brFeatures;
	ArrayList<Feature> features = new ArrayList<Feature>();
	SimplifyCT sim;
	
	ArrayList<Integer> criticalPts;
	int lastSaddle;
	
	private int countFeatures(boolean max) {
		ReebGraphData data = sim.data;
		int no = 0;
		
		for(int i = 0;i < sim.order.length;i ++) {
			Branch br = sim.branches[sim.order.array[i]];
			if(max) {
				if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
					no ++;
				}
			} else {
				if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					no ++;
				}
			}
		}
		return no;
	}
	
	private int countFeatures(boolean max, float th) {
		ReebGraphData data = sim.data;
		int no = 0;
		
		for(int i = 0;i < sim.order.length;i ++) {
			Branch br = sim.branches[sim.order.array[i]];
			float fn = data.nodes[br.to].fn - data.nodes[br.from].fn;
			if(max) {
				if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
					if(fn >= th) {
						no ++;
					}
				}
			} else {
				if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					if(fn >= th) {
						no ++;
					}
				}
			}
		}
		return no;
	}
	HashSet<Integer> featureSet = new HashSet<Integer>();
	HashMap<Integer, Integer> cps;
	
	private void initFeatures(boolean max) {
		int nf = brFeatures.length;
		if(nf > sim.order.length) {
			nf = sim.order.length;
			brFeatures = new Feature[nf];
		}
		int pos = sim.order.length - 1;
		ReebGraphData data = sim.data;
		lastSaddle = -1;
		if(max) {
			for(int i = 0;i < data.noNodes;i ++) {
				if(data.nodes[i].type == ReebGraphData.SADDLE) {
					lastSaddle = i;
					break;
				}
			}
		} else {
			for(int i = data.noNodes - 1;i >= 0;i --) {
				if(data.nodes[i].type == ReebGraphData.SADDLE) {
					lastSaddle = i;
					break;
				}
			}
		}
		// Added by Harish
		lastSaddle = -1;
		cps = new HashMap<Integer, Integer>();
		boolean root = true;
		int no = 0;
		while(no < nf) {
			int bno = sim.order.get(pos); 
			Branch br = sim.branches[bno];
			if(max) {
				if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
					cps.put(br.to, no);
					brFeatures[no] = new Feature();
					brFeatures[no].v = data.nodes[br.to].v;
					brFeatures[no].exFn = data.nodes[br.to].fn;
					brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
					brFeatures[no].sadFn = data.nodes[br.from].fn;
					if(root) {
						if(lastSaddle == -1) {
							// single edge
							brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
							brFeatures[no].sadFn = data.nodes[br.from].fn;
						} else {
							brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[lastSaddle].fn;
							brFeatures[no].sadFn = data.nodes[lastSaddle].fn;
						}
						root = false;
					}
					no ++;
				}
			} else {
				if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					cps.put(br.from, no);
					brFeatures[no] = new Feature();
					brFeatures[no].v = data.nodes[br.from].v;
					brFeatures[no].exFn = data.nodes[br.from].fn;
					brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
					brFeatures[no].sadFn = data.nodes[br.to].fn;
					if(root) {
						if(lastSaddle == -1) {
							// single edge
							brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
							brFeatures[no].sadFn = data.nodes[br.to].fn;
						}else {
							brFeatures[no].wt = data.nodes[lastSaddle].fn - data.nodes[br.from].fn;
							brFeatures[no].sadFn = data.nodes[lastSaddle].fn;
						}
						root = false;
					}
					no ++;
				}
			}
			pos --;
		}
	}

	
	private void getFeatures(boolean max) {
		for(int i = 0;i < sim.branches.length;i ++) {
			if(sim.removed[i]) {
				continue;
			}
			Branch br = sim.branches[i];
			int fno = -1;
			if(max && cps.get(br.to) != null) {
				fno = cps.get(br.to);
			} else if(!max && cps.get(br.from) != null) {
				fno = cps.get(br.from);
			}
			if(fno == -1) {
				continue;
			}
			brFeatures[fno].br = i;
			featureSet.add(i);
		}
	}
	
	boolean less(int v1, int v2) {
		if(sim.data.nodes[v1].fn < sim.data.nodes[v2].fn) {
			return true;
		}
		if(sim.data.nodes[v1].fn > sim.data.nodes[v2].fn) {
			return false;
		}
		if(v1 < v2) {
			return true;
		}
		return false;
	}
	
	private void populateFeature(int f, boolean max) {
		int bno = brFeatures[f].br;
		ArrayList<Integer> queue = new ArrayList<Integer>();
		queue.add(bno);
		while(queue.size() > 0) {
			int b = queue.remove(0);
			if(b != bno && featureSet.contains(b)) {
				Utilities.er("Can this now happen??");
				continue;
			}
			Branch br = sim.branches[b];
			brFeatures[f].arcs.addAll(br.arcs);
			for(int i = 0;i < br.children.length;i ++) {
				int bc = br.children.get(i);
				queue.add(bc);
			}
		}
	}


	public void simplify(int noFeatures, String rg, String simrg, boolean maxAsFeature, Function fn) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
		System.out.println("Finished simplification");

		int totalFeatures = countFeatures(maxAsFeature);
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeatures(maxAsFeature);
		
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn,totalFeatures, noFeatures);
		
		getFeatures(maxAsFeature);
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i, maxAsFeature);
		}
		
		System.out.println("Updated features");
	}
	
	/**
	 * 
	 * @param rg
	 * @param simrg
	 * @param maxAsFeature
	 * @param fn
	 * @param th Assumes that the values are normalized between 0 and 1
	 */
	public void simplify(String rg, String simrg, boolean maxAsFeature, Function fn, float th) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
		System.out.println("Finished simplification");

		int totalFeatures = countFeatures(maxAsFeature);
		int noFeatures = countFeatures(maxAsFeature, th);
		System.out.println("No. of features: " + noFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeatures(maxAsFeature);
		
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn,totalFeatures, noFeatures);
		
		getFeatures(maxAsFeature);
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i, maxAsFeature);
		}
		
		System.out.println("Updated features");
	}
	
	double [] fn;
	double [] ct;
	double [] ex;
	int [] exv;


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
		Arc arc1 = sim.data.arcs[o1];
		Arc arc2 = sim.data.arcs[o2];
		int v1 = arc1.from;
		int v2 = arc2.from;
		
		if(sim.data.nodes[v1].fn < sim.data.nodes[v2].fn || (sim.data.nodes[v1].fn == sim.data.nodes[v2].fn && sim.data.nodes[v1].v < sim.data.nodes[v2].v)) {
			return -1;
		}
		return 1;
	}
	
	public void writePersistence(String perFile, String extremaFile) {
		try {
			PrintStream pr = new PrintStream(perFile);
			PrintStream er = new PrintStream(extremaFile);
			
			float max = brFeatures[0].wt;
			for(int i = 1;i < brFeatures.length;i ++) {
				max = Math.max(max,brFeatures[i].wt);
			}
			
			// boundary case
			if(brFeatures.length == 2) {
				exType[0] = CTAlgorithm.MAXIMUM | CTAlgorithm.MINIMUM; 
			}
			for(int i = 0;i < brFeatures.length;i ++) {
				float per = (max == 0)?1:brFeatures[i].wt / max;
				double pp = 0;
				if(ct != null && ct[i] != 0) {
					pp = fn[i] / ct[i];
				}
				if(per > 1) {
					Utilities.er("!!!!!!!!!!!!!!!!!!!!!!!");
				}
				pr.println(i + " " + per + " " + brFeatures[i].wt + " " + brFeatures[i].exFn + " " + pp + " " + exType[i]);
//				er.println(i + " " + brFeatures[i].v);
				er.println(i + " " + exv[i] + " " + exType[i]);
			}
			pr.close();
			er.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/***************************************************************************************************************************************/
	
	
	public void simplify(int noFeatures, String rg, String simrg, Function fn) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
		System.out.println("Finished simplification");

		int totalFeatures = sim.order.length;
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeatures();
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i);
		}
		
		System.out.println("Updated features");
	}
	
	public void simplify(String rg, String simrg, Function fn, float th) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
//		System.out.println("Finished simplification");

		int totalFeatures = sim.order.length;
		int noFeatures = countFeatures(th);
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeatures();
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i);
		}
		
//		System.out.println("Updated features");
	}
	
	public void simplify(ReebGraphData rgData, String simrg, Function fn, float th) {
		sim = new SimplifyCT();
		sim.setInput(rgData);
		sim.simplify(fn);
//		System.out.println("Finished simplification");

		int totalFeatures = sim.order.length;
		int noFeatures = countFeatures(th);
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeatures();
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i);
		}
		
//		System.out.println("Updated features");
	}
	
	public Feature [] getFeatures() {
		return brFeatures;
	}
	
	private int countFeatures(float th) {
		ReebGraphData data = sim.data;
		int no = 0;
		
		for(int i = 0;i < sim.order.length;i ++) {
			Branch br = sim.branches[sim.order.array[i]];
			float fn = data.nodes[br.to].fn - data.nodes[br.from].fn;
			if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
				if(fn >= th) {
					no ++;
				}
			} else if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
				if(fn >= th) {
					no ++;
				}
			}
		}
		return (no == 0)?1:no;
	}
	
	public void simplifyNoPart(String rg, String simrg, Function fn, float th) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
		System.out.println("Finished simplification");

		int totalFeatures = sim.order.length;
		int noFeatures = countFeaturesNoPart(th);
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeaturesNoPart();
		
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn,totalFeatures, noFeatures);
				
		getFeaturesNoPart();
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeatureNoPart(i);
		}
		
		System.out.println("Updated features");
	}
	
	
	 public void simplifyNoPart(int noFeatures, String rg, String simrg, Function fn) {
	    sim = new SimplifyCT();
	    sim.setInput(rg);
	    sim.simplify(fn);
	    System.out.println("Finished simplification");

	    int totalFeatures = sim.order.length;
//	    int noFeatures = countFeaturesNoPart(th);
	    noFeatures = Math.min(noFeatures, totalFeatures);
	    brFeatures = new Feature[noFeatures];
	    
	    initFeaturesNoPart();
	    
	    sim = new SimplifyCT();
	    sim.setInput(rg);
	    sim.simplify(fn,totalFeatures, noFeatures);
	        
	    getFeaturesNoPart();
	    
	    for(int i = 0;i < brFeatures.length;i ++) {
	      populateFeatureNoPart(i);
	    }
	    
	    System.out.println("Updated features");
	  }
	
	private void getFeaturesNoPart() {
		for(int i = 0;i < sim.branches.length;i ++) {
			if(sim.removed[i]) {
				continue;
			}
			Branch br = sim.branches[i];
			int fno = -1;
			if(cps.get(br.to) != null) {
				fno = cps.get(br.to);
			} 
			if(cps.get(br.from) != null) {
				fno = cps.get(br.from);
			}
			if(fno == -1) {
				continue;
			}
			brFeatures[fno].br = i;
			featureSet.add(i);
		}
	}
	
	private int countFeaturesNoPart(float th) {
		ReebGraphData data = sim.data;
		int no = 0;
		
		for(int i = 0;i < sim.order.length;i ++) {
			Branch br = sim.branches[sim.order.array[i]];
			float fn = data.nodes[br.to].fn - data.nodes[br.from].fn;
			if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
				if(fn >= th) {
					no ++;
				}
			} 
			if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
				if(fn >= th) {
					no ++;
				}
			}
		}
		if(no == 0) {
			no = 2;
		}
		return no;
	}
	
	private void initFeatures() {
		featureSet.clear();
		int nf = brFeatures.length;
		if(nf > sim.order.length) {
			nf = sim.order.length;
			brFeatures = new Feature[nf];
		}
		int pos = sim.order.length - 1;
		ReebGraphData data = sim.data;
		cps = new HashMap<Integer, Integer>();
		int root = 0;
		int no = 0;
		while(no < nf) {
			int bno = sim.order.get(pos); 
			Branch br = sim.branches[bno];

			if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
				featureSet.add(bno);
				cps.put(br.to, no);
				brFeatures[no] = new Feature();
				brFeatures[no].v = data.nodes[br.to].v;
				brFeatures[no].exFn = data.nodes[br.to].fn;
				brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
				brFeatures[no].sadFn = data.nodes[br.from].fn;
				brFeatures[no].type = CTAlgorithm.MAXIMUM;
				if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					brFeatures[no].type |= CTAlgorithm.MINIMUM;
					root ++;
				}
				brFeatures[no].br = bno;
				no ++;
			} else if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
				featureSet.add(bno);
				cps.put(br.from, no);
				brFeatures[no] = new Feature();
				brFeatures[no].v = data.nodes[br.from].v;
				brFeatures[no].exFn = data.nodes[br.from].fn;
				brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
				brFeatures[no].sadFn = data.nodes[br.to].fn;
				brFeatures[no].br = bno;
				brFeatures[no].type = CTAlgorithm.MINIMUM;
				no ++;
			}
			pos --;
		}
		if(root != 1) {
			if(root > 1) {
				Utilities.er("Can there be more than one root???");
			} else {
				Utilities.er("Where has the root gone missing");
			}
		}
	}

	
	private void initFeaturesNoPart() {
		featureSet.clear();
		int nf = brFeatures.length;
		if(nf > sim.order.length) {
			nf = sim.order.length;
			brFeatures = new Feature[nf];
		}
		int pos = sim.order.length - 1;
		ReebGraphData data = sim.data;
		cps = new HashMap<Integer, Integer>();
		int root = 0;
		int no = 0;
		while(no < nf) {
			int bno = sim.order.get(pos); 
			Branch br = sim.branches[bno];

			if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
				featureSet.add(bno);
				cps.put(br.to, no);
				brFeatures[no] = new Feature();
				brFeatures[no].v = data.nodes[br.to].v;
				brFeatures[no].exFn = data.nodes[br.to].fn;
				brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
				brFeatures[no].sadFn = data.nodes[br.from].fn;
				brFeatures[no].type = CTAlgorithm.MAXIMUM;
				if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
//					brFeatures[no].type |= CTAlgorithm.MINIMUM;
					root ++;
				}
				brFeatures[no].br = bno;
				no ++;
			} 
			
			if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
				featureSet.add(bno);
				cps.put(br.from, no);
				brFeatures[no] = new Feature();
				brFeatures[no].v = data.nodes[br.from].v;
				brFeatures[no].exFn = data.nodes[br.from].fn;
				brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
				brFeatures[no].sadFn = data.nodes[br.to].fn;
				brFeatures[no].br = bno;
				brFeatures[no].type = CTAlgorithm.MINIMUM;
				no ++;
			}
			pos --;
		}
		if(root != 1) {
			if(root > 1) {
				Utilities.er("Can there be more than one root???");
			} else {
				Utilities.er("Where has the root gone missing");
			}
		}
	}

	
	private void populateFeature(int f) {
		int bno = brFeatures[f].br;
		ArrayList<Integer> queue = new ArrayList<Integer>();
		queue.add(bno);
		while(queue.size() > 0) {
			int b = queue.remove(0);
			if(b!= bno && featureSet.contains(b)) {
				continue;
			}
			Branch br = sim.branches[b];
			brFeatures[f].arcs.addAll(br.arcs);
			for(int i = 0;i < br.children.length;i ++) {
				int bc = br.children.get(i);
				queue.add(bc);
			}
		}
	}
	
	private void populateFeatureNoPart(int f) {
		int bno = brFeatures[f].br;
		ArrayList<Integer> queue = new ArrayList<Integer>();
		queue.add(bno);
		while(queue.size() > 0) {
			int b = queue.remove(0);
			if(b!= bno && featureSet.contains(b)) {
				continue;
			}
			Branch br = sim.branches[b];
			brFeatures[f].arcs.addAll(br.arcs);
			for(int i = 0;i < br.children.length;i ++) {
				int bc = br.children.get(i);
				queue.add(bc);
			}
		}
	}
	
	byte [] exType;
	public void updatePartition(String part, String simpart) {
		int [] arcMap = new int[sim.data.noArcs];
		Arrays.fill(arcMap, -1);
		
//		fn = new double[sim.data.noArcs];
//		ct = new double[sim.data.noArcs];
		ex = new double[brFeatures.length];
		exv = new int[brFeatures.length];
		exType = new byte[brFeatures.length];
		
		Arrays.sort(brFeatures, new Comparator<Feature>() {
			@Override
			public int compare(Feature o1, Feature o2) {
				if(o2.wt > o1.wt) {
					return 1;
				}
				return -1;
			}
		});
		
		for(int i = 0;i < brFeatures.length;i ++) {
			for(int j = 0;j < brFeatures[i].arcs.length;j ++) {
				int arc = brFeatures[i].arcs.get(j);
				arcMap[arc] = i;
			}
			ex[i] = brFeatures[i].exFn;
			exv[i] = brFeatures[i].v;
			exType[i] = brFeatures[i].type; 
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(part));
			PrintWriter p = new PrintWriter(simpart);
			
			int n = Integer.parseInt(reader.readLine().trim());
			p.println(n);
			
			for(int i = 0;i < n;i ++) {
				int e = Integer.parseInt(reader.readLine().trim());
				int newe = -1;
				if(e != -1) {
					newe = arcMap[e];	
				}
				// boundary condition
				if(brFeatures.length == 2) {
					newe = 0;
				}
				p.println(newe);
			}
			reader.close();
			p.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public int [] getUpdatedPartition(String part) {
		int [] arcMap = new int[sim.data.noArcs];
		Arrays.fill(arcMap, -1);
		
		ex = new double[brFeatures.length];
		exv = new int[brFeatures.length];
		exType = new byte[brFeatures.length];
		
		Arrays.sort(brFeatures, new Comparator<Feature>() {
			@Override
			public int compare(Feature o1, Feature o2) {
				if(o2.wt > o1.wt) {
					return 1;
				}
				return -1;
			}
		});
		
		for(int i = 0;i < brFeatures.length;i ++) {
			for(int j = 0;j < brFeatures[i].arcs.length;j ++) {
				int arc = brFeatures[i].arcs.get(j);
				arcMap[arc] = i;
			}
			ex[i] = brFeatures[i].exFn;
			exv[i] = brFeatures[i].v;
			exType[i] = brFeatures[i].type; 
		}
		try {
			BufferedReader reader = new BufferedReader(new FileReader(part));
			int n = Integer.parseInt(reader.readLine().trim());
			int [] reg = new int[n];
			
			for(int i = 0;i < n;i ++) {
				int e = Integer.parseInt(reader.readLine().trim());
				int newe = -1;
				if(e != -1) {
					newe = arcMap[e];	
				}
				// boundary condition
				if(brFeatures.length == 2) {
					newe = 0;
				}
				reg[i] = newe;
			}
			reader.close();
			return reg;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	
	///////////
	public void simplifyRootChildren(String rg, String simrg, Function fn, float th) {
		sim = new SimplifyCT();
		sim.setInput(rg);
		sim.simplify(fn);
		System.out.println("Finished simplification");

		int totalFeatures = sim.order.length;
		int noFeatures = countFeaturesRootChildren(th);
		noFeatures = Math.min(noFeatures, totalFeatures);
		brFeatures = new Feature[noFeatures];
		
		initFeaturesRootChildren();
		
		for(int i = 0;i < brFeatures.length;i ++) {
			populateFeature(i);
		}
		
		System.out.println("Updated features");
	}
	
	private int countFeaturesRootChildren(float th) {
		ReebGraphData data = sim.data;
		int no = 0;
		int root = sim.order.array[sim.order.length - 1];
		if(sim.branches[root].parent != -1) {
			Utilities.er("incorrect root!! don't be lazy and write code properly");
		}
		for(int i = 0;i < sim.order.length;i ++) {
			Branch br = sim.branches[sim.order.array[i]];
			if(br.parent == root) {
				float fn = data.nodes[br.to].fn - data.nodes[br.from].fn;
				if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
					if(fn >= th) {
						no ++;
					}
				} else if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					if(fn >= th) {
						no ++;
					}
				}
			}
		}
		return (no == 0)?1:no;
	}
	
	private void initFeaturesRootChildren() {
		featureSet.clear();
		int nf = brFeatures.length;
		if(nf > sim.order.length) {
			nf = sim.order.length;
			brFeatures = new Feature[nf];
		}
		int pos = sim.order.length - 1;
		ReebGraphData data = sim.data;
		cps = new HashMap<Integer, Integer>();
		int root = 0;
		int rootBr = sim.order.array[sim.order.length - 1];
		int no = 0;
		while(no < nf) {
			int bno = sim.order.get(pos); 
			Branch br = sim.branches[bno];
			if(rootBr == bno || br.parent == rootBr) {
				if(data.nodes[br.to].type == ReebGraphData.MAXIMUM) {
					featureSet.add(bno);
					cps.put(br.to, no);
					brFeatures[no] = new Feature();
					brFeatures[no].v = data.nodes[br.to].v;
					brFeatures[no].exFn = data.nodes[br.to].fn;
					brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
					brFeatures[no].sadFn = data.nodes[br.from].fn;
					brFeatures[no].type = CTAlgorithm.MAXIMUM;
					if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
						brFeatures[no].type |= CTAlgorithm.MINIMUM;
						root ++;
					}
					brFeatures[no].br = bno;
					no ++;
				} else if(data.nodes[br.from].type == ReebGraphData.MINIMUM) {
					featureSet.add(bno);
					cps.put(br.from, no);
					brFeatures[no] = new Feature();
					brFeatures[no].v = data.nodes[br.from].v;
					brFeatures[no].exFn = data.nodes[br.from].fn;
					brFeatures[no].wt = data.nodes[br.to].fn - data.nodes[br.from].fn;
					brFeatures[no].sadFn = data.nodes[br.to].fn;
					brFeatures[no].br = bno;
					brFeatures[no].type = CTAlgorithm.MINIMUM;
					no ++;
				}
			}
			pos --;
		}
		if(root != 1) {
			if(root > 1) {
				Utilities.er("Can there be more than one root???");
			} else {
				Utilities.er("Where has the root gone missing");
			}
		}
	}

}

