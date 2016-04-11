/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import java.io.Serializable;

public class ContourTree implements Serializable {
    private static final long serialVersionUID = 1L;
	
	class Node implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		int v;
		int [] next;
		int nsize = 0;
		int [] prev;
		int psize = 0;
		public Node(boolean split) {
			if(split) {
				next = new int[maxStar];
				prev = new int[1];
			} else {
				next = new int[1];
				prev = new int[maxStar];
			}
		}
	}
	
	Node [] nodesJoin;
	Node [] nodesSplit;
	int nv; 
	int maxStar;
	int [] joinNodes;
	int [] splitNodes;
	int jct = 0;
	int sct = 0;
	
	public ContourTree(int noVertices, int maxStar, int nt) {
		nv = noVertices;
		this.maxStar = maxStar;

		nodesJoin = new Node[nv + nt + nt];
		nodesSplit = new Node[nv + nt + nt];
		
		joinNodes = new int[nv + nt + nt];
		splitNodes = new int[nv + nt + nt];
		
		q = new int[nv + nt + nt];
	}

	public void addJoinArc(int from, int to) {
		Node nv1 = nodesJoin[from];
		if(nv1 == null) {
			nv1 = new Node(false);
			nv1.v = from;
			nodesJoin[from] = nv1;
			joinNodes[jct ++] = from;
		}
		
		Node nv2 = nodesJoin[to];
		if(nv2 == null) {
			nv2 = new Node(false);
			nv2.v = to;
			nodesJoin[to] = nv2;
			joinNodes[jct ++] = to;
		}
		
		nv1.next[nv1.nsize ++] = to;
		nv2.prev[nv2.psize ++] = from;
	}
	
	public void addSplitArc(int from, int to) {
		Node nv1 = nodesSplit[from];
		if(nv1 == null) {
			nv1 = new Node(true);
			nv1.v = from;
			nodesSplit[from] = nv1;
			splitNodes[sct ++] = from;
		}
		
		Node nv2 = nodesSplit[to];
		if(nv2 == null) {
			nv2 = new Node(true);
			nv2.v = to;
			nodesSplit[to] = nv2;
			splitNodes[sct ++] = to;
		}
		
		nv1.next[nv1.nsize ++] = to;
		nv2.prev[nv2.psize ++] = from;
	}
	
	int [] q;
	public void mergeTrees(StoreReebGraph rg) {
		int front = 0;
		int back = 0;
		for(int x = 0;x < jct; x++) {
			int v = joinNodes[x];
			Node jn = nodesJoin[v];
			Node sn = nodesSplit[v];
			if(sn.nsize + jn.psize == 1) {
				q[back ++] = v;
			}
		}
		
		while(back > front + 1) {
			int xi = q[front ++];
			Node jn = nodesJoin[xi];
			Node sn = nodesSplit[xi];
			
			if(sn.nsize == 0 && sn.psize == 0) {
				if(!(jn.nsize == 0 && jn.psize == 0)) {
					System.out.println("Shoudnt happen!!!");
					System.exit(0);
				}
				continue;
			}
			if(sn.nsize == 0) {
				if(sn.psize > 1) {
					System.out.println("Can this happen too???");
					System.exit(0);
				}
				int xj = sn.prev[0];
				remove(xi, nodesJoin);
				remove(xi, nodesSplit);
				int fr = xj;
				int to = xi;
				if(fr > nv) {
					if((fr & 1) == 1) {
						fr --;
					}
				}
				if(to > nv) {
					if((to & 1) == 1) {
						to --;
					}
				}
				rg.addArc(fr, to);
				if(nodesSplit[xj].nsize + nodesJoin[xj].psize == 1) {
					q[back ++] = xj;
				}
			} else {
				if(jn.nsize > 1) {
					System.out.println("Can this happen too???");
					System.exit(0);
				}
				if(jn.nsize == 0) {
					System.out.println("ContourTree.mergeTrees()");
				}
				int xj = jn.next[0];
				
				remove(xi, nodesJoin);
				remove(xi, nodesSplit);
				
				int fr = xi;
				int to = xj;
				if(fr > nv) {
					if((fr & 1) == 1) {
						fr --;
					}
				}
				if(to > nv) {
					if((to & 1) == 1) {
						to --;
					}
				}
				rg.addArc(fr, to);

				if(nodesSplit[xj].nsize + nodesJoin[xj].psize == 1) {
					q[back ++] = xj;
				}
			}
		}
	}
	
	private void remove(int xi, Node [] nodeArray) {
		Node jn = nodeArray[xi];
		
		if(jn.psize == 1 && jn.nsize == 1) {
			int p = jn.prev[0];
			int n = jn.next[0];
			jn.psize = 0;
			jn.nsize  = 0;
			
			Node pn = nodeArray[p];
			Node nn = nodeArray[n];
			
			removeAndAdd(pn.next, pn.nsize, xi, nn.v);
			removeAndAdd(nn.prev, nn.psize, xi, pn.v);
		} else if(jn.psize == 0 && jn.nsize == 1) {
			int n = jn.next[0];
			jn.nsize = 0;
			Node nn = nodeArray[n];
			remove(nn.prev, nn.psize, xi);
			nn.psize --;
		} else if(jn.psize == 1 && jn.nsize == 0) {
			int p = jn.prev[0];
			jn.psize = 0;
			Node pn = nodeArray[p];
			remove(pn.next, pn.nsize, xi);
			pn.nsize --;
		} else {
			System.out.println("Can this too happen??????");
			System.exit(0);
		}
	}

	private void remove(int[] arr, int arrSize, int xi) {
		for(int i = 0;i < arrSize;i ++) {
			if(arr[i] == xi) {
				if(i != arrSize - 1) {
					arr[i] = arr[arrSize - 1];
				}
				return;
			}
		}
		System.out.println("Shouldn't happen");
		System.exit(0);
	}

	private void removeAndAdd(int[] arr, int arrSize, int rem, int add) {
		for(int i = 0;i < arrSize;i ++) {
			if(arr[i] == rem) {
				arr[i] = add;
				return;
			}
		}
		System.out.println("Shouldn't happen");
		System.exit(0);
	}
}
