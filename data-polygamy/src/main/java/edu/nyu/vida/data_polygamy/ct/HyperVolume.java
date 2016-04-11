/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import static edu.nyu.vida.data_polygamy.utils.Utilities.splitString;
import edu.nyu.vida.data_polygamy.utils.Utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;

public class HyperVolume implements Function, Serializable {
    private static final long serialVersionUID = 1L;

	public MyIntList[] regions;

	public float[] fnVals;
	public float[] fn;
	public float[] fnVertices;
	boolean min = true;

	public HyperVolume(String rgFile, String partFile, String scalarFile) {
		try {
			BufferedReader f = new BufferedReader(new FileReader(rgFile));
			String s = f.readLine();
			String[] r = splitString(s);
			int noNodes = Integer.parseInt(r[0].trim());
			int noArcs = Integer.parseInt(r[1].trim());
			fnVals = new float[noNodes];
			int noMin = 0;
			int noMax = 0;
			for (int i = 0; i < noNodes; i++) {
				s = f.readLine();
				r = splitString(s);
				fnVals[i] = Float.parseFloat(r[1].trim());
				if (r[2].equalsIgnoreCase("MINIMA")) {
					noMin++;
				} else if (r[2].equalsIgnoreCase("MAXIMA")) {
					noMax++;
				}
			}
			f.close();
			if (noMin == 1) {
				min = false;
			} else if (noMax == 1) {
				min = true;
			} else {
				Utilities.er("Not possible!!!!");
			}
			regions = new MyIntList[noArcs];
			BufferedReader ip = new BufferedReader(new FileReader(partFile));
			int nv = Integer.parseInt(ip.readLine().trim());
			int[] cols = new int[nv];
			for (int i = 0; i < cols.length; i++) {
				cols[i] = Integer.parseInt(ip.readLine().trim());
				if (regions[cols[i]] == null) {
					regions[cols[i]] = new MyIntList();
				}
				regions[cols[i]].add(i);
			}
			ip.close();

			BufferedReader scalar = new BufferedReader(new FileReader(scalarFile));
			fnVertices = new float[nv];
			for (int i = 0; i < nv; i++) {
				String line = scalar.readLine().trim();
				try {
					fnVertices[i] = Float.parseFloat(line);
				} catch (NumberFormatException e) {
					e.printStackTrace();
					fnVertices[i] = 0;
					System.exit(0);
				}
			}
			scalar.close();

			initVolumes(cols);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// TODO remove later
	private void initVolumes(int[] cols) {
	}

	@Override
	public void init(float[] fn, Branch[] br) {
		this.fn = fn;
		for (int brNo = 0; brNo < fn.length; brNo++) {
			update(br, brNo);
		}
	}

	@Override
	public void update(Branch[] br, int brNo) {
		int saddleVertex = -1;
		if (min) {
			saddleVertex = br[brNo].to;
		} else {
			saddleVertex = br[brNo].from;
		}
		fn[brNo] = update(br, brNo, saddleVertex);
	}

	float update(Branch[] br, int brNo, int saddleVertex) {
		float val = 0;
		for (int i = 0; i < br[brNo].arcs.length; i++) {
			int ano = br[brNo].arcs.get(i);
			if(regions[ano] == null) {
				return 0;
			}
			for(int j = 0;j < regions[ano].length;j ++) {
				int v = regions[ano].get(j);
				float vv = fnVertices[v] - fnVals[saddleVertex];
				val += Math.abs(vv);
			}
		}
		for (int i = 0; i < br[brNo].children.length; i++) {
			int child = br[brNo].children.get(i);
			val += update(br, child, saddleVertex);
		}
		return val;
	}

	@Override
	public void branchRemoved(Branch[] br, int brNo, boolean[] invalid) {

	}
}
