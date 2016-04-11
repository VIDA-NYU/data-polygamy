/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import edu.nyu.vida.data_polygamy.ct.ReebGraphData.Node;
import edu.nyu.vida.data_polygamy.utils.DisjointSets;
import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class OrderMergeTree implements Serializable {
    private static final long serialVersionUID = 1L;

	public class Component implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		ArrayList<Integer> nodes = new ArrayList<Integer>();
		int root = -1;
		int ex = -1;
		HashMap<Integer, Integer> pair = new HashMap<Integer, Integer>(); 
	}
	
	public ReebGraphData rg;
	public ReebGraphData tree;
	public boolean max;
	public ArrayList<Integer> extrema = new ArrayList<Integer>();
	
	DisjointSets dj;
	Component [] comps;
	HashMap<Integer, Integer> compMap;
	
	public OrderMergeTree() {
		
	}
	
	public OrderMergeTree(String rgFile, boolean max) {
		rg = new ReebGraphData(rgFile);
		this.max = max;
		for(int i = 0;i < rg.noNodes;i ++) {
			if(max && rg.nodes[i].type == ReebGraphData.MAXIMUM) {
				extrema.add(i);
			}
			if(!max && rg.nodes[i].type == ReebGraphData.MINIMUM) {
				extrema.add(i);
			}
		}
	}
	
	/*
	 * Assumes that the extrema are with respect to original vertex ids
	 */
	public void reorderTree(ArrayList<Integer> extrema) {
		tree = new ReebGraphData();
		HashSet<Integer> nodeSet = new HashSet<Integer>();
		for(int ex: extrema) {
			int v = rg.nodeMap.get(ex);
			if(max) {
				nodeSet.add(v);
				while(rg.nodes[v].prev.size() > 0) {
					if(rg.nodes[v].prev.size() != 1) {
						Utilities.er("Cannot have 2 lower neighbours");
					}
					int e = rg.nodes[v].prev.get(0);
					v = rg.arcs[e].from;
					if(nodeSet.contains(v)) {
						break;
					}
					nodeSet.add(v);
				}
			} else {
				nodeSet.add(v);
				while(rg.nodes[v].next.size() > 0) {
					if(rg.nodes[v].next.size() != 1) {
						Utilities.er("Cannot have 2 upper neighbours");
					}
					int e = rg.nodes[v].next.get(0);
					v = rg.arcs[e].to;
					if(nodeSet.contains(v)) {
						break;
					}
					nodeSet.add(v);
				}
			}
		}
		tree.noNodes = nodeSet.size();
		tree.noArcs = Math.max(0,tree.noNodes - 1);
		tree.nodes = new ReebGraphData.Node[tree.noNodes];
		tree.arcs = new ReebGraphData.Arc[tree.noArcs];
		tree.nodeMap.clear();
		
		int i = 0;
		for(int v: nodeSet) {
			tree.nodes[i] = tree.new Node();
			tree.nodes[i].v = rg.nodes[v].v;
			tree.nodes[i].fn = rg.nodes[v].fn;
			tree.nodes[i].type = rg.nodes[v].type;
			++i;
		}
		Arrays.sort(tree.nodes, new Comparator<ReebGraphData.Node>() {
			@Override
			public int compare(Node o1, Node o2) {
				if(o1.fn < o2.fn) {
					return -1;
				}
				if(o1.fn > o2.fn) {
					return 1;
				}
				return (o1.v - o2.v);
			}
		});
		for(i = 0;i < tree.nodes.length; i ++) {
			tree.nodeMap.put(tree.nodes[i].v, i);
		}
		int ct = 0;
		for(i = 0;i < tree.nodes.length; i ++) {
			if(max) {
				int in = rg.nodeMap.get(tree.nodes[i].v);
				if(rg.nodes[in].prev.size() > 0) {
					tree.arcs[ct] = tree.new Arc();
					int e = rg.nodes[in].prev.get(0);
					int j = rg.arcs[e].from;
					tree.arcs[ct].from = tree.nodeMap.get(rg.nodes[j].v);
					tree.arcs[ct].to = i;
					tree.arcs[ct].id = ct;
					tree.nodes[tree.arcs[ct].from].next.add(ct);
					tree.nodes[tree.arcs[ct].to].prev.add(ct);
					ct ++;
				}
			} else {
				int in = rg.nodeMap.get(tree.nodes[i].v);
				if(rg.nodes[in].next.size() > 0) {
					tree.arcs[ct] = tree.new Arc();
					int e = rg.nodes[in].next.get(0);
					int j = rg.arcs[e].to;
					tree.arcs[ct].from = i;
					tree.arcs[ct].to = tree.nodeMap.get(rg.nodes[j].v);
					tree.arcs[ct].id = ct;
					tree.nodes[tree.arcs[ct].from].next.add(ct);
					tree.nodes[tree.arcs[ct].to].prev.add(ct);
					ct ++;
				}
			}
		}
		if(ct != tree.arcs.length) {
			Utilities.er("some error");
		}
		
		findComponents();
	}
	
	void findComponents() {
		if(rg == null) {
			return;
		}
		dj = new DisjointSets();
		Set<Integer> matched = tree.nodeMap.keySet();
		HashSet<Integer> remaining = new HashSet<Integer>(); 
		for(int i = 0;i < rg.nodes.length;i ++) {
			if(matched.contains(rg.nodes[i].v)) {
				continue;
			}
			remaining.add(i);
			
			int v1 = i;
			int v2;
			if(max) {
				if(rg.nodes[i].prev.size() == 0) {
					continue;
				}
				int e = rg.nodes[i].prev.get(0);
				v2 = rg.arcs[e].from;
			} else {
				if(rg.nodes[i].next.size() == 0) {
					continue;
				}				
				int e = rg.nodes[i].next.get(0);
				v2 = rg.arcs[e].to;
			}
			if(matched.contains(rg.nodes[v2].v)) {
				continue;
			}
			dj.union(dj.find(v1), dj.find(v2));
		}
		
		HashSet<Integer> cset = new HashSet<Integer>();
		for(int v: remaining) {
			cset.add(dj.find(v));
		}
		compMap = new HashMap<Integer, Integer>();
		int cin = 0;
		for(int c: cset) {
			compMap.put(c, cin);
			cin ++;
		}
		int noComp = cset.size();
		comps = new Component[noComp];
		for(int v: remaining) {
			cin = compMap.get(dj.find(v));
			if(comps[cin] == null) {
				comps[cin] = new Component();
			}
			comps[cin].nodes.add(v);
			int vv;
			if(max && rg.nodes[v].prev.size() > 0) {
				vv = rg.arcs[rg.nodes[v].prev.get(0)].from;
			} else if(!max && rg.nodes[v].next.size() > 0) {
				vv = rg.arcs[rg.nodes[v].next.get(0)].to;
			} else {
				continue;
			}
			if(matched.contains(rg.nodes[vv].v) || (max && rg.nodes[vv].prev.size() == 0)
					|| (!max && rg.nodes[vv].next.size() == 0)) {
				if(comps[cin].root != -1) {
					Utilities.er("some problem with component");
				}
				comps[cin].root = vv;
			}
		}
		for(Component comp: comps) {
			double mval = 0;
			int ex = -1;
			for(int v: comp.nodes) {
				if(this.max) {
					double val = rg.nodes[v].fn;
					if(ex == -1) {
						mval = val;
						ex = v;
					} else {
						if(mval < val) {
							mval = val;
							ex = v;
						} else if(mval == val && ex < v) {
							mval = val;
							ex = v;
						}
					}
				} else {
					double val = rg.nodes[v].fn;
					if(ex == -1) {
						mval = val;
						ex = v;
					} else {
						if(mval > val) {
							mval = val;
							ex = v;
						} else if(mval == val && ex > v) {
							mval = val;
							ex = v;
						}
					}
				}
			}
			if(ex == -1) {
				Utilities.er("component doesn't have extreme point?????");
			}
			comp.ex = ex;
			processComponent(comp);
		}
	}

	public void processComponent(final Component c) {
		if(max) {
			Collections.sort(c.nodes, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					double val1 = rg.nodes[o1].fn;
					double val2 = rg.nodes[o2].fn;
					if(val1 < val2) {
						return -1;
					}
					if(val1 > val2) {
						return 1;
					}
					return (o1 - o2);
				}
			});
		} else {
			Collections.sort(c.nodes, new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					double val1 = rg.nodes[o1].fn;
					double val2 = rg.nodes[o2].fn;
					if(val1 < val2) {
						return 1;
					}
					if(val1 > val2) {
						return -1;
					}
					return (o2 - o1);
				}
			});
		}
		
		HashSet<Integer> reached = new HashSet<Integer>();
		reached.add(c.root);
		for(int i = c.nodes.size() - 1;i >= 0;i --) {
			int v = c.nodes.get(i);
			int vv = v;
			while(!reached.contains(vv)) {
				reached.add(vv);
				if(max) {
					vv = rg.arcs[rg.nodes[vv].prev.get(0)].from;
				} else {
					vv = rg.arcs[rg.nodes[vv].next.get(0)].to;
				}
			}
			if(vv != v) {
				c.pair.put(v,vv);
			}
		}
	}
	
	public double getPersistence(int ex) {
		int v1 = rg.nodeMap.get(ex);
		if(compMap.get(dj.find(v1)) == null) {
			return -1;
		}
		int cin = compMap.get(dj.find(v1));
		int v2 = comps[cin].pair.get(v1);
		
		return Math.abs(rg.nodes[v1].fn - rg.nodes[v2].fn);
	}
	
	public int getSaddlePair(int ex) {
		int v1 = rg.nodeMap.get(ex);
		if(compMap.get(dj.find(v1)) == null) {
			return -1;
		}
		int cin = compMap.get(dj.find(v1));
		int v2 = comps[cin].pair.get(v1);
		
		return rg.nodes[v2].v;
	}
	
	public double getMaxSimplification() {
		if(comps == null) {
			return 0;
		}
		double val = 0;
		for(Component comp: comps) {
			val = Math.max(val, Math.abs(rg.nodes[comp.root].fn - rg.nodes[comp.ex].fn));
			assert comp.pair.get(comp.ex) == comp.root; 
		}
		return val;
	}
}

