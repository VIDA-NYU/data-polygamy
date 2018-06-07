/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.exception.ConvergenceException;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import edu.nyu.vida.data_polygamy.ct.Function;
import edu.nyu.vida.data_polygamy.ct.GraphInput;
import edu.nyu.vida.data_polygamy.ct.MergeTrees;
import edu.nyu.vida.data_polygamy.ct.MergeTrees.TreeType;
import edu.nyu.vida.data_polygamy.ct.MyIntList;
import edu.nyu.vida.data_polygamy.ct.Persistence;
import edu.nyu.vida.data_polygamy.ct.ReebGraphData;
import edu.nyu.vida.data_polygamy.ct.SimplifyFeatures;
import edu.nyu.vida.data_polygamy.ct.SimplifyFeatures.Feature;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class TopologicalIndex implements Serializable {
	
    private static final long serialVersionUID = 1L;
    
    //private static final double thresholdRatio = 0.2;

    public static class Attribute implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public int id;
        public Int2ObjectOpenHashMap<ArrayList<SpatioTemporalVal>> data =
                new Int2ObjectOpenHashMap<ArrayList<SpatioTemporalVal>>();
        public Int2ObjectOpenHashMap<Integer> thresholdStTime =
                new Int2ObjectOpenHashMap<Integer>();
        public Int2ObjectOpenHashMap<Integer> thresholdEnTime =
                new Int2ObjectOpenHashMap<Integer>();
        public Int2ObjectOpenHashMap<Float> minThreshold =
                new Int2ObjectOpenHashMap<Float>();
        public Int2ObjectOpenHashMap<Float> maxThreshold =
                new Int2ObjectOpenHashMap<Float>();
        public IntOpenHashSet nodeSet = new IntOpenHashSet();
        
        public void copy(Attribute att) {
            this.id = att.id;
            data.putAll(att.data);
            nodeSet.addAll(att.nodeSet);
        }
    }
	
	public static class Event implements Comparable<Event>, Serializable {
	    private static final long serialVersionUID = 1L;
	    
		public int time;
		public boolean positive;
		public int id;
		
		@Override
		public int compareTo(Event o) {
			return time - o.time;
		}
		
		@Override
		public boolean equals(Object obj) {
			Event r = (Event) obj;
			return r.time == time && r.id == id && positive == r.positive;
		}
		
		@Override
		public int hashCode() {
			return (time + " " + id + " " + positive).hashCode();
		}
	}
	
	public static class PersistencePoints implements Serializable {
	    private static final long serialVersionUID = 1L;
	    
		public DoubleArrayList ex = new DoubleArrayList();
		public DoubleArrayList sad = new DoubleArrayList();
		public void addPersistencePoint(Feature f, TreeType tree) {
			double pt = f.exFn;
			double s = f.sadFn;
			if(pt < 100) {
				pt *= 1;
			}
			if (tree == TreeType.JoinTree && f.sadFn < f.exFn) {
				pt = f.sadFn;
				s = f.exFn;
			}
			ex.add(pt);
			sad.add(s);
		}
	}
	
	static TreeType [] types = {TreeType.JoinTree, TreeType.SplitTree};
	
	int attribute;
	int spatialRes, tempRes;
    boolean is2D = false;
    public boolean empty = true;
    
    public int stTime = Integer.MAX_VALUE;
    public int enTime = 0;
    public int nv;
    
    Int2ObjectOpenHashMap<GraphInput> functions = new Int2ObjectOpenHashMap<GraphInput>();
    Int2ObjectOpenHashMap<Feature[]> minIndex = new Int2ObjectOpenHashMap<Feature[]>();
    Int2ObjectOpenHashMap<Feature[]> maxIndex = new Int2ObjectOpenHashMap<Feature[]>();
    
    public TopologicalIndex() {}
	
    public TopologicalIndex(int spatialRes, int tempRes, int nv) {
        this.empty = false;
        this.nv = nv;
        this.tempRes = tempRes;
        this.spatialRes = spatialRes;
        
        if (spatialRes != FrameworkUtils.CITY)
            is2D = true;
    }
    
    public Int2ObjectOpenHashMap<Feature[]> getIndex(boolean min) {
        if (min)
            return minIndex;
        else
            return maxIndex;
    }

    public int createIndex(Attribute att, int[][] edges2D) {
        this.attribute = att.id;
        //if (att.data.size() == 0) return 1;
		for (int t = 0; t < types.length; t++) {
			TreeType tree = types[t];
			boolean min = true;
			if (tree == TreeType.SplitTree) {
				min = false;
			}
			try {
				for (int tempBin : att.data.keySet()) {
				    //System.out.println("Time: " + tempBin);
				    ArrayList<SpatioTemporalVal> stArr = att.data.get(tempBin);
					int actualVertices = stArr.size();
					
					//if (actualVertices == 0) continue;
					
					int localSt = stArr.get(0).getTemporal();
					int localEnd = stArr.get(stArr.size() - 1).getTemporal();
					stTime = Math.min(stTime, localSt);
                    enTime = Math.max(enTime, localEnd);
                    
                    att.thresholdStTime.put(tempBin, new Integer(stTime));
                    att.thresholdEnTime.put(tempBin, new Integer(enTime));
					
					GraphInput tf = functions.get(tempBin);
					if (tf == null) {
					    if (is2D) {
	                        tf = new TimeSeries2DFunction(stArr, att.nodeSet, edges2D,
	                                this.nv, this.tempRes, localSt, localEnd);
					    }
	                    else
	                        tf = new TimeSeriesFunction(stArr);
						functions.put(tempBin, tf);
					}
					
					MergeTrees ct = new MergeTrees();
					ct.computeTree(tf, tree);
					ReebGraphData data = ct.output(tree);

					if (data.noArcs == 0) {
					    System.err.println("Empty Attribute: " + att.id);
					    return 1;
					}
					
					Function fn = new Persistence(data);
					SimplifyFeatures sim = new SimplifyFeatures();
					sim.simplify(data, null, fn, 0.01f);

					Feature[] f = sim.brFeatures;
					if (min) {
						if(f[0].sadFn != data.nodes[0].fn) {
							Utilities.er("I have no idea what is happening!!!!! Version 3");
						}
						f[0].v = data.nodes[0].v;
					} else {
						if(is2D) {
							actualVertices = tf.getFnVertices().length;
						} else if(tf.getFnVertices().length != actualVertices) {
							Utilities.er("Its time you quit!!");
						}
						
						if(f[0].v == actualVertices) {
							// new root added
							int to = data.arcs[data.noArcs - 1].to;
							int from = data.arcs[data.noArcs - 1].from;
							if(data.nodes[from].v != actualVertices) {
								Utilities.er("I have no idea what is happening!!!!!");
							}
							f[0].v = data.nodes[to].v;
							if(data.nodes[to].fn != f[0].exFn) {
								Utilities.er("I have no idea what is happening!!!!! Version 2");
							}
						}
					}
					//System.out.println("creating contour tree for " + tempBin);
					if (min) {
						minIndex.put(tempBin, f);
					} else {
						maxIndex.put(tempBin, f);
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return 1;
			}
		}
		
        return 0;
	}
    
    public ArrayList<byte[]> queryEvents(float th, boolean outlier, Attribute att, String threshold) {
        return queryEvents(th, outlier, att, threshold, false);
    }
	
	public ArrayList<byte[]> queryEvents(float th, boolean outlier, Attribute att, String threshold, boolean print) {
		
	    att.minThreshold.clear();
	    att.maxThreshold.clear();
	    
	    int timeSteps = FrameworkUtils.getTimeSteps(this.tempRes, this.stTime, this.enTime);
	    ArrayList<byte[]> results = new ArrayList<byte[]>();
	    if (timeSteps == 0) return results;
	    for (int i = 0; i < this.nv; i++) {
	        byte[] data = new byte[timeSteps];
	        Arrays.fill(data, FrameworkUtils.nonEvent);
	        results.add(data);
	    }
		
		for(int t = 0;t < types.length;t ++) {
			TreeType tree = types[t];
			boolean min = true;
			if (tree == TreeType.SplitTree) {
				min = false;
			}
			Int2ObjectOpenHashMap<Feature[]> index = getIndex(min);
			
			PersistencePoints perVals = new PersistencePoints();
			for (int tempBin : att.data.keySet()) {
				Feature []f = index.get(tempBin);
				
				if (f.length == 0) continue;
				
				double pth = th * f[0].wt;
				if (!outlier) {
				    try {
				        if (threshold.isEmpty()) {
				            pth = getThreshold(f);
				        } else {
				            pth = Double.parseDouble(threshold);
				        }
				    } catch (ConvergenceException e) {
				        continue;
				    }
				}
				for (int i = 0; i < f.length; i++) {
					if(f[i].wt >= pth) {
						perVals.addPersistencePoint(f[i], tree);
					} else {
						break;
					}
				}
				if (!outlier) {
					Collections.sort(perVals.ex);
					int no = perVals.ex.size();
					double[] vals = new double[no];
					for (int i = 0; i < vals.length; i++) {
						vals[i] = perVals.ex.get(i);
					}
					if(vals.length == 0) {
						continue;
					}
					double eventTh = min?vals[vals.length - 1]:vals[0];
					getEvents(results, functions.get(tempBin),
					        index.get(tempBin), min, eventTh, print);
					perVals = new PersistencePoints();
					
					if (min) {
					    att.minThreshold.put(tempBin, new Float(eventTh));
					} else {
					    att.maxThreshold.put(tempBin, new Float(eventTh));
					}
				}
			}
			if (outlier) {
				Collections.sort(perVals.ex);
				int no = perVals.ex.size();
				double[] vals = new double[no];
				
				for (int i = 0; i < vals.length; i++) {
					vals[i] = perVals.ex.get(i);
				}
				if(vals.length == 0) {
					continue;
				}
				double eventTh = min?vals[vals.length - 1]:vals[0];
				
			    if (threshold.isEmpty()) {
			        eventTh = iqOutlierTh(vals, min);
                } else {
                    eventTh = Double.parseDouble(threshold);
                }
			    getEvents(results, eventTh, index, min, att);
			}
		}
		return results;
	}
	
	public static double epsilon = 0.00001;
	private double iqOutlierTh(double[] vals, boolean min) {
		DescriptiveStatistics ds = new DescriptiveStatistics(vals);
		double fq = ds.getPercentile(25);
		double tq = ds.getPercentile(75);
		double iqr = tq - fq;
		
		if(min) {
			double th = fq - 1.5 * iqr;
			return (th - epsilon);
		} else {
			double th = tq + 1.5 * iqr;
			return (th + epsilon);
		}
	}
	
	void getEvents(ArrayList<byte[]> events, double eventTh,
			Int2ObjectOpenHashMap<Feature[] > featureMap, boolean min, Attribute att) {
		// getting events using merge tree
		for (int tempBin : att.data.keySet()) {
			Feature[] features = featureMap.get(tempBin);
			GraphInput tf = functions.get(tempBin);
			getEvents(events, tf, features, min, eventTh, false);
			
			if (min) {
                att.minThreshold.put(tempBin, new Float(eventTh));
            } else {
                att.maxThreshold.put(tempBin, new Float(eventTh));
            }
		}
	}
	
	private void getEvents(ArrayList<byte[]> events, GraphInput tf,
	        Feature[] features, boolean min, double eventTh, boolean print) {
		float[] fnVertices = tf.getFnVertices();
//		nv = 1;
//		if (is2D) {
//			nv = ((TimeSeries2DFunction)tf).nv;
//		}
		IntOpenHashSet set = new IntOpenHashSet();
		
		/*int numberThresholdValues = 0;
		for (Feature f: features) {
		    double pt = f.exFn;
		    if (min) {
                if (f.sadFn < f.exFn) {
                    pt = f.sadFn;
                }
		    }
		    if (pt == eventTh) {
		        numberThresholdValues++;
		    }
		}
		double ratio = numberThresholdValues / (double) features.length;*/
		
		for(Feature f: features) {
			float pt = f.exFn;

			if (min) {
				if (f.sadFn < f.exFn) {
					pt = f.sadFn;
				}
	            
				//if (ratio >= thresholdRatio && pt == eventTh) {
                //    continue;
                //}
				
				if (pt > eventTh) {
                    continue;
                }
				int exv = f.v;
				
				IntArrayList queue = new IntArrayList();
				queue.add(exv);
				while(queue.size() > 0) {
					int vin = queue.remove(0);
					if(set.contains(vin)) {
						continue;
					}
					set.add(vin);
					pt = fnVertices[vin];
					
					int tid = vin / nv;
                    int time = tf.getTime(tid);
                    int spatial = vin % nv;
                    
                    int index = FrameworkUtils.getTimeSteps(this.tempRes,
                            this.stTime, time);
					
					if(pt <= eventTh) {
					    
					    byte[] spatialEvents = events.get(spatial);
					    if (spatialEvents[index-1] == FrameworkUtils.positiveEvent) {
                            spatialEvents[index-1] = FrameworkUtils.nonEvent;
                        } else {
                            spatialEvents[index-1] = FrameworkUtils.negativeEvent;
                        }
                        events.set(spatial, spatialEvents);
					    
					    if (print) {
					        // October 15th, 2011 to October 31st, 2011
				            if ((time >= 1318636800) && (time <= 1320105599)) {
				                System.out.print(FrameworkUtils.getTemporalStr(FrameworkUtils.HOUR, time) + "\t");
				                System.out.print(eventTh + ", " + pt);
				                System.out.println("");
				            }
					    }
						
						MyIntList star = tf.getStar(vin);
						for(int i = 0;i < star.length;i ++) {
							if(!set.contains(star.array[i])) {
								queue.add(star.array[i]);
							}
						}
					}
				}
			} else {
				
				//if (ratio >= thresholdRatio && pt == eventTh) {
                //    continue;
                //}
                if (pt < eventTh) {
                    continue;
                }

				int exv = f.v;
				
				IntArrayList queue = new IntArrayList();
				queue.add(exv);
				while(queue.size() > 0) {
					int vin = queue.remove(0);
					if(set.contains(vin)) {
						continue;
					}

					set.add(vin);
					pt = fnVertices[vin];
					
					int tid = vin / nv;
                    int time = tf.getTime(tid);
                    int spatial = vin % nv;
                    
                    int index = FrameworkUtils.getTimeSteps(this.tempRes,
                            this.stTime, time);
					
					if(pt >= eventTh) {
                        byte[] spatialEvents = events.get(spatial);
                        if (spatialEvents[index-1] == FrameworkUtils.negativeEvent) {
                            spatialEvents[index-1] = FrameworkUtils.nonEvent;
                        } else {
                            spatialEvents[index-1] = FrameworkUtils.positiveEvent;
                        }
                        events.set(spatial, spatialEvents);
                        
                        if (print) {
                            // October 15th, 2011 to October 31st, 2011
                            if ((time >= 1318636800) && (time <= 1320105599)) {
                                System.out.print(FrameworkUtils.getTemporalStr(FrameworkUtils.HOUR, time) + "\t");
                                System.out.print(eventTh + ", " + pt);
                                System.out.println("");
                            }
                        }
						
						MyIntList star = tf.getStar(vin);
						for(int i = 0;i < star.length;i ++) {
							if(!set.contains(star.array[i])) {
								queue.add(star.array[i]);
							}
						}
					}
				}
			}
		}
	}

	public double getThreshold(Feature []f) {
	    
	    KMeansPlusPlusClusterer<DoublePoint> kmeans = new KMeansPlusPlusClusterer<DoublePoint>(2,1000);
	    ArrayList<DoublePoint> pts = new ArrayList<DoublePoint>();
		
		if(f.length < 2) {
			return f[0].wt * 0.4;
		}
		for (int i = 0; i < f.length; i++) {
			DoublePoint dpt = new DoublePoint(new double[] {f[i].wt});
			pts.add(dpt);
		}
		List<CentroidCluster<DoublePoint>> clusters = kmeans.cluster(pts);
		
		double maxp = 0;
		double minp = 0;
		int ct = 0;
		for(CentroidCluster<DoublePoint> c: clusters) {
			double mp = 0;
			double mnp = Double.MAX_VALUE;
			for(DoublePoint dpt: c.getPoints()) {
				double [] pt = dpt.getPoint();
				mp = Math.max(mp,pt[0]);
				mnp = Math.min(mnp,pt[0]);
			}
			if(mp > maxp) {
				maxp = mp;
				minp = mnp;
			}
			ct ++;
		}
		if(ct > 2) {
			Utilities.er("Can there be > 2 clusters?");
		}
		return minp;
	}

}

