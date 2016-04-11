/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import static edu.nyu.vida.data_polygamy.utils.Utilities.splitString;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;

public class HyperVolumeApprox implements Function, Serializable {
    private static final long serialVersionUID = 1L;

	public float [] fnVals;
	public float [] fn; 
	public float [] vol;
	public float [] brVol;
	
	public HyperVolumeApprox(String rgFile, String partFile) {
		try {
			BufferedReader f = new BufferedReader(new FileReader(rgFile));
			String s = f.readLine();
			String [] r = splitString(s);
			int noNodes = Integer.parseInt(r[0].trim());
			int noArcs = Integer.parseInt(r[1].trim());
			fnVals = new float[noNodes];
			for(int i = 0;i < noNodes;i ++) {
				s = f.readLine();
				r = splitString(s);
				fnVals[i] = Float.parseFloat(r[1].trim());
			}
			f.close();
			
			BufferedReader ip = new BufferedReader(new FileReader(partFile));
			int nv = Integer.parseInt(ip.readLine().trim());
			int [] cols = new int[nv];
			for(int i = 0;i < cols.length;i ++) {
				cols[i] = Integer.parseInt(ip.readLine().trim());
			}
			ip.close();
			vol = new float[noArcs];
			brVol = new float[noArcs];
			initVolumes(cols);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initVolumes(int[] cols) {
		for(int i = 0;i < cols.length;i ++) {
			if(cols[i] == -1) {
				continue;
			}
			vol[cols[i]] ++;
		}
		int sum = 0;
		for(int i = 0;i < vol.length;i ++) {
			sum += vol[i];
		}
		System.out.println("Sum : " + sum);
	}

	@Override
	public void init(float[] fn, Branch[] br) {
		this.fn = fn;
		for(int i = 0;i < fn.length;i ++) {
			update(br, i);
		}
	}

	@Override
	public void update(Branch[] br, int brNo) {
		brVol[brNo] = 0;
		for(int i = 0;i < br[brNo].arcs.length;i ++) {
			brVol[brNo] += vol[br[brNo].arcs.get(i)];
		}
		for(int i = 0;i < br[brNo].children.length;i ++) {
			int child = br[brNo].children.get(i);
			brVol[brNo] += vol(br,child);
		}
		float fnDiff = fnVals[br[brNo].to] - fnVals[br[brNo].from]; 
		fn[brNo] = fnDiff * brVol[brNo];
	}

	private float vol(Branch[] br, int brNo) {
		float val = 0;
		for(int i = 0;i < br[brNo].arcs.length;i ++) {
			val += vol[br[brNo].arcs.get(i)];
		}
		for(int i = 0;i < br[brNo].children.length;i ++) {
			int child = br[brNo].children.get(i);
			val = val + vol(br,child);
		}
		return val;
	}

	@Override
	public void branchRemoved(Branch[] br, int brNo, boolean [] invalid) {
		// TODO Auto-generated method stub
		
	}
}
