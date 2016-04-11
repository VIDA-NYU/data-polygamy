/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import java.io.Serializable;



public class DisjointSetsInt implements Serializable {
    private static final long serialVersionUID = 1L;

	int [] set;

	public DisjointSetsInt(int no) {
		set = new int [no];
	}

	public void clear() {
		int no = set.length;
		set = new int [no];
	}
	/**
	 * Union two disjoint sets using the height heuristic. root1 and root2 are
	 * distinct and represent set names.
	 * 
	 * @param root1
	 *            the root of set 1.
	 * @param root2
	 *            the root of set 2.
	 */
	public void union(int root1, int root2) {
		if (root1 == root2)
			return;

		int r1 = set[root1];
		int r2 = set[root2];

		if (r2 < r1) {
			// root2 is deeper
			// Make root2 new root
			set[root1] = root2;
		} else {
			if (r1 == r2) {
				// Update height if same
				r1--;
				set[root1] = r1;
			}
			// Make root1 new root
			set[root2] = root1;
		}
	}

	/**
	 * Perform a find with path compression.
	 * 
	 * @param x
	 *            the element being searched for.
	 * @return the set containing x.
	 */
	public int find(int x) {
		int f = set[x];
		if (f < 1) {
			return x;
		} else {
			int xx = find(f);
			set[x] = xx;
			return xx;
		}
	}


	// Test main; all finds on same output line should be identical
	public static void main(String[] args) {
		int numElements = 128;
		int numInSameSet = 16;

		DisjointSetsInt ds = new DisjointSetsInt(numElements + 1);
		int set1, set2;

		for (int k = 1; k < numInSameSet; k *= 2) {
			for (int j = 0; j + k < numElements; j += 2 * k) {
				set1 = ds.find(j + 1);
				set2 = ds.find(j + k + 1);
				ds.union(set1, set2);
			}
		}

		for (int i = 0; i < numElements; i++) {
			System.out.print(ds.find(i + 1) + "*");
			if (i % numInSameSet == numInSameSet - 1)
				System.out.println();
		}
		System.out.println();
	}
}
