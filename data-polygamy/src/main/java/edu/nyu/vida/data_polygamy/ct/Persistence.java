/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import static edu.nyu.vida.data_polygamy.utils.Utilities.splitString;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;

public class Persistence implements Function, Serializable {
    private static final long serialVersionUID = 1L;

	public float [] fnVals;
	public float [] fn; 
	public Persistence(String rgFile) {
		try {
			BufferedReader f = new BufferedReader(new FileReader(rgFile));
			String s = f.readLine();
			String [] r = splitString(s);
			int noNodes = Integer.parseInt(r[0].trim());
			fnVals = new float[noNodes];
			for(int i = 0;i < noNodes;i ++) {
				s = f.readLine();
				r = splitString(s);
				fnVals[i] = Float.parseFloat(r[1].trim());
			}
			f.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public Persistence(ReebGraphData data) {
		try {
			int noNodes = data.noNodes;
			fnVals = new float[noNodes];
			for(int i = 0;i < noNodes;i ++) {
				fnVals[i] = data.nodes[i].fn;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	@Override
	public void init(float[] fn, Branch[] br) {
		this.fn = fn;
		for(int i = 0;i < fn.length;i ++) {
			fn[i] = fnVals[br[i].to] - fnVals[br[i].from];
		}
	}

	@Override
	public void update(Branch[] br, int brNo) {
		fn[brNo] = fnVals[br[brNo].to] - fnVals[br[brNo].from];
	}

	@Override
	public void branchRemoved(Branch[] br, int brNo, boolean[] invalid) {
		// TODO Auto-generated method stub
		
	}

}
