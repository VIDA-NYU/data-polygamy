/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.resolution;

import java.awt.geom.Path2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Random;

public class GridIndex {

	class Bound {
		double x1,x2;
		double y1,y2;
	}
	
	// assume fixed width, height
	public class Node {
		public ArrayList<Integer> polys = new ArrayList<Integer>();
	}
	
	int xs,ys;
	
	public Node [][] grid;
	double width, height;
	Bound bound = new Bound();
	ArrayList<Path2D.Double> polygons;
	
	public GridIndex(int xsize,int ysize) {
		xs = xsize;
		ys = ysize;
		grid = new Node[xs][ys];
		for(int i = 0;i < xs;i ++) {
			for(int j = 0;j < ys;j ++) {
				grid[i][j] = new Node();
			}
		}
	}
	
	public void buildGrid(ArrayList<Path2D.Double> polygons) {
		this.polygons = polygons;
		
		double maxx = -Double.MAX_VALUE;
		double maxy = -Double.MAX_VALUE;
		double minx = Double.MAX_VALUE;
		double miny = Double.MAX_VALUE;
		Bound [] bounds = new Bound[polygons.size()];
		int i = 0;
		for(Path2D.Double p: polygons) {
			Rectangle2D rect = p.getBounds2D();
			bounds[i] = new Bound();
			bounds[i].x1 = rect.getMinX();
			bounds[i].y1 = rect.getMinY();
			bounds[i].x2 = rect.getMaxX();
			bounds[i].y2 = rect.getMaxY();
			
			minx = Math.min(bounds[i].x1, minx);
			miny = Math.min(bounds[i].y1, miny);
			maxx = Math.max(bounds[i].x2, maxx);
			maxy = Math.max(bounds[i].y2, maxy);
			i ++;
		}
		width = (maxx - minx) / xs;
		height = (maxy - miny) / ys;
		
		bound.x1 = minx;
		bound.x2 = maxx;
		bound.y1 = miny;
		bound.y2 = maxy;
		
		i = 0;
		for(Bound b: bounds) {
			int stx = getXIndex(b.x1);
			int enx = getXIndex(b.x2) + 1; 
			int sty = getYIndex(b.y1);
			int eny = getYIndex(b.y2) + 1;
			
			if(enx >= xs) {
				enx = xs - 1;
			}
			if(eny >= ys) {
				eny = ys - 1;
			}
			
			for(int x = stx;x <= enx;x ++) {
				for(int y = sty;y <= eny;y ++) {
					grid[x][y].polys.add(i);
				}
			}
			i ++;
		}
	}

	private int getXIndex(double x) {
		double in = (x - bound.x1) / width;
		return (int) in;
	}
	
	private int getYIndex(double y) {
		double in = (y - bound.y1) / height;
		return (int) in;
	}

	public int getRegion(double x, double y) {
		int stx = getXIndex(x);
		int sty = getYIndex(y);

		if(stx >= xs) {
			stx = xs - 1;
		}
		if(sty >= ys) {
			sty = ys - 1;
		}

        if(stx < 0) {
            stx = 0;
        }
        if(sty < 0) {
            sty = 0;
        }
        
		for(int p: grid[stx][sty].polys) {
			Path2D.Double poly = polygons.get(p);
			if(poly.contains(x, y)) {
				return p;
			}
		}
		return -1;
	}
	
	private int getRegionBF(double x, double y) {
		int ct = 0;
		for(Path2D.Double poly: polygons) {
			if(poly.contains(x, y)) {
				return ct;
			}
			ct ++;
		}
		return -1;
	}

	
	public void test() {
		int maxb = 0;
		for(int x = 0;x < xs;x ++) {
			for(int y = 0;y < ys;y ++) {
				maxb = Math.max(maxb, grid[x][y].polys.size());
			}
		}
		System.out.println(maxb);
		
		
		Random r = new Random();
		int n = 100000;
		double [] x = new double[n];
		double [] y = new double[n];
		for(int i = 0;i < n;i ++) {
			x[i] = r.nextDouble() * width + bound.x1;
			y[i] = r.nextDouble() * height + bound.y1;
		}
		
		int [] rg = new int[n];
		int [] rb = new int[n];
		
		long st = System.currentTimeMillis();
		for(int i = 0;i < n;i ++) {
			rg[i] = getRegion(x[i],y[i]);
		}
		long timeGrid = System.currentTimeMillis() - st;
		
		st = System.currentTimeMillis();
		for(int i = 0;i < n;i ++) {
			rb[i] = getRegionBF(x[i],y[i]);
		}
		long timeBruteForce = System.currentTimeMillis() - st;
		
		int ct = 0;
		for(int i = 0;i < n;i ++) {
			if(rg[i] != rb[i]) {
				System.out.println("brute force and grid index results don't match!!!!");
			}
			if(rg[i] != -1) {
				ct ++;
			}
		}
		System.out.println("No. of points within polygon set: " + ct);
		System.out.println("Time taken using grid index: " + timeGrid);
		System.out.println("Time taken using brute force: " + timeBruteForce);
		System.out.println("Speedup: " + (double) timeBruteForce / (double) timeGrid);
	}


}
