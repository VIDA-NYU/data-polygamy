/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.exp;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import edu.nyu.vida.data_polygamy.relationship_computation.CorrelationReducer;
import edu.nyu.vida.data_polygamy.utils.SpatialGraph;
import edu.nyu.vida.data_polygamy.ctdata.SpatioTemporalVal;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex.Attribute;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TimeSeriesStats;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class TaxiSyntheticExp {
    
    static String[] dataAttributes = {"count-db_idx", "unique-medallion_id",
        "avg-miles", "avg-fare"};
    HashSet<String> dataAttributesHashSet = new HashSet<String>(Arrays.asList(dataAttributes));
    
    float alpha = 0.05f;
    
    public HashMap<String, Attribute> loadData(String aggregatesFile, int year) {
        HashMap<String, Attribute> attributes = new HashMap<String, Attribute>();
        String[] s = null;
        try {
            BufferedReader buf = new BufferedReader(new FileReader(aggregatesFile));
            s = FrameworkUtils.getLine(buf, ",");
            while (true) {
                if (s == null) {
                    break;
                }
                String attr = FrameworkUtils.splitString(s[0], ":")[1].trim();
                //System.out.println("Attribute: " + attr);
                Attribute a = new Attribute();
                a.nodeSet.add(0);
                s = FrameworkUtils.getLine(buf, ",");
                if(s != null && s.length > 0 && s[0].toLowerCase().startsWith("spatial")) {
                    s = FrameworkUtils.getLine(buf, ",");
                }
                if(s == null || s.length == 0) {
                    System.out.println("Empty: ---------------------- " + attr);
                }
                while (s != null && s.length > 0) {
                    int month = Integer.parseInt(FrameworkUtils.splitString(s[0], ":")[1].trim());
                    s = FrameworkUtils.getLine(buf, ",");
                    HashSet<SpatioTemporalVal> set = new HashSet<SpatioTemporalVal>();
                    while (s != null && s.length == 2) {
                        if (month/100 == year) {
                            int time = Integer.parseInt(s[0]);
                            if ((time >= 1330560000) || (time < 1330473600)) {
                                if (year == 2011) {
                                    // adjusting weekdays with 2012
                                    time = time - (86400);
                                } else { // 2012
                                    time = time - (31536000);
                                }
                                float value = Float.parseFloat(s[1]);
                                SpatioTemporalVal val = new SpatioTemporalVal(0, time, value);
                                set.add(val);
                            }
                        }
                        s = FrameworkUtils.getLine(buf, ",");
                    }
                    if (set.size() > 0) {
                        ArrayList<SpatioTemporalVal> arr = new ArrayList<SpatioTemporalVal>(set);                   
                        Collections.sort(arr);
                        a.data.put(month, arr);
                    }
                }
                
                if (dataAttributesHashSet.contains(attr)) {
                    attributes.put(attr, a);
                }
                s = FrameworkUtils.getLine(buf, ",");
            }
            buf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return attributes;
    }
    
    public HashMap<String, Attribute> load2DData(String aggregateFile, String graphFile, int year) {
        HashMap<String, Attribute> attributes = new HashMap<String, Attribute>();
        IntOpenHashSet nodeSet = new IntOpenHashSet();
        String[] s = null;
        try {
            BufferedReader buf = new BufferedReader(new FileReader(aggregateFile));
            s = FrameworkUtils.getLine(buf, ",");
            //System.out.println(s[0]);
            while (true) {
                if (s == null) {
                    break;
                }
                String attr = FrameworkUtils.splitString(s[0], ":")[1].trim();
                Attribute a = attributes.get(attr);
                if(a == null) {
                    a = new Attribute();
                    attributes.put(attr, a);
                }
                s = FrameworkUtils.getLine(buf, ":");
                int sid = Integer.parseInt(s[1].trim());
                nodeSet.add(sid);
                s = FrameworkUtils.getLine(buf, ",");
                while (s != null && s.length > 0) {
                    int month = Integer.parseInt(FrameworkUtils.splitString(s[0], ":")[1].trim());
                    s = FrameworkUtils.getLine(buf, ",");
                    HashSet<SpatioTemporalVal> set = new HashSet<SpatioTemporalVal>();
                    while (s != null && s.length == 2) {
                        if (month/100 == year) {
                            int time = Integer.parseInt(s[0]);
                            if ((time >= 1330560000) || (time < 1330473600)) {
                                if (year == 2011) {
                                    // adjusting weekdays with 2012
                                    time = time - (86400);
                                } else { // 2012
                                    time = time - (31536000);
                                }
                                float value = Float.parseFloat(s[1]);
                                SpatioTemporalVal val = new SpatioTemporalVal(sid, time, value);
                                set.add(val);
                            }
                        }
                        s = FrameworkUtils.getLine(buf, ",");
                    }
                    if (set.size() > 0) {
                        ArrayList<SpatioTemporalVal> monthlyArr = a.data.get(month);
                        if(monthlyArr == null) {
                            monthlyArr = new ArrayList<>();
                            a.data.put(month, monthlyArr);
                        }
                        monthlyArr.addAll(set);
                    }
                }
                
                if (dataAttributesHashSet.contains(attr)) {
                    attributes.put(attr, a);
                }
                s = FrameworkUtils.getLine(buf, ",");
            }
            buf.close();
            
            for(Attribute a: attributes.values()) {
                for(ArrayList<SpatioTemporalVal> arr: a.data.values()) {
                    Collections.sort(arr);
                }
                a.nodeSet = nodeSet;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return attributes;
    }
    
    public void run(String dataFile, String graphFile, String polygonsFile, boolean is2D) throws IOException {
        
        SpatialGraph spatialGraph = new SpatialGraph();
        ArrayList<Integer[]> edges = new ArrayList<Integer[]>();
        int spatialRes = 0;
        int nv = 1;
        
        if (is2D) {
            try {
                spatialGraph.init(polygonsFile, graphFile);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            
            spatialRes = FrameworkUtils.NBHD;
            
            BufferedReader reader = new BufferedReader(new FileReader(graphFile));
            String [] s = Utilities.splitString(reader.readLine().trim());
            nv = Integer.parseInt(s[0].trim());
            int ne = Integer.parseInt(s[1].trim());
            for(int i = 0;i < ne;i ++) {
                s = Utilities.splitString(reader.readLine().trim());
                int v1 = Integer.parseInt(s[0].trim());
                int v2 = Integer.parseInt(s[1].trim());
                if(v1 == v2) {
                    continue;
                }
                Integer[] arr = new Integer[2];
                arr[0] = v1;
                arr[1] = v2;
                edges.add(arr);
            }
            reader.close();
            
        } else {
            spatialRes = FrameworkUtils.CITY;
        }
        
        // 2011
        HashMap<String, Attribute> attributes2011 =
                (!is2D) ? this.loadData(dataFile, 2011) : this.load2DData(dataFile, graphFile, 2011);
        ArrayList<TopologicalIndex> dataIndex2011 = new ArrayList<>();
        for(int i = 0; i < dataAttributes.length; i++) {
            TopologicalIndex index = this.createIndex(attributes2011, dataAttributes[i], spatialRes, nv, edges);
            dataIndex2011.add(index);
        }
        
        // 2012
        HashMap<String, Attribute> attributes2012 =
                (!is2D) ? this.loadData(dataFile, 2012) : this.load2DData(dataFile, graphFile, 2012);
        ArrayList<TopologicalIndex> dataIndex2012 = new ArrayList<>();
        for(int i = 0; i < dataAttributes.length; i++) {
            TopologicalIndex index = this.createIndex(attributes2012, dataAttributes[i], spatialRes, nv, edges);
            dataIndex2012.add(index);
        }
        
        boolean outlier = false;
        float th = 0.90f;
                
        if (!is2D) {
            for(int i = 0; i < dataAttributes.length; i++) {
                System.out.println("Attribute: " + dataAttributes[i]);
                
                ArrayList<byte[]> e1 = dataIndex2011.get(i).queryEvents(
                        th, outlier, attributes2011.get(dataAttributes[i]), "");
                ArrayList<byte[]> e2 = dataIndex2012.get(i).queryEvents(
                        th, outlier, attributes2012.get(dataAttributes[i]), "");
                
                TopologyTimeSeriesWritable t1 = 
                        new TopologyTimeSeriesWritable(0, 0, i, e1.get(0),
                                dataIndex2011.get(i).stTime,
                                dataIndex2011.get(i).enTime,
                                dataIndex2011.get(i).getNbPosEvents(0),
                                dataIndex2011.get(i).getNbNegEvents(0),
                                dataIndex2011.get(i).getNbNonEvents(0),
                                outlier);
                
                TopologyTimeSeriesWritable t2 = 
                        new TopologyTimeSeriesWritable(0, 0, i, e2.get(0),
                                dataIndex2012.get(i).stTime,
                                dataIndex2012.get(i).enTime,
                                dataIndex2012.get(i).getNbPosEvents(0),
                                dataIndex2012.get(i).getNbNegEvents(0),
                                dataIndex2012.get(i).getNbNonEvents(0),
                                outlier);
                
                int temporal = FrameworkUtils.HOUR;
                TimeSeriesStats stats = CorrelationReducer.getStats(temporal, t1, t2, false);
                
                stats.computeScores();
                if (!stats.isIntersect())
                    return;
                
                float alignedScore = stats.getRelationshipScore();
                
                System.out.println("Score: " + alignedScore);
                System.out.println("Strength: " + stats.getRelationshipStrength());
    
                int repetitions = 1000;
                float pValue = 0; 
                for (int j = 0; j < repetitions; j++) {
                    stats = new TimeSeriesStats();
                    stats.add(CorrelationReducer.getStats(FrameworkUtils.HOUR, t1, t2, true));
                    
                    stats.computeScores();
                    
                    float mcScore = stats.getRelationshipScore();
                    
                    if (alignedScore > 0) {
                        if (mcScore >= alignedScore)
                            pValue += 1;
                    }
                    else {
                        if (mcScore <= alignedScore)
                            pValue += 1;
                    }
                    if (pValue > (alpha*repetitions)) break; // pruning
                }
                
                pValue = pValue/((float)(repetitions));
                
                if (pValue <= alpha)  {
                    System.out.println("p-value: " + pValue);
                } else {
                    System.out.println("p-value: " + pValue + " [not significant]");
                }
            }
        } else {
            for(int i = 0; i < dataAttributes.length; i++) {
                System.out.println("Attribute: " + dataAttributes[i]);
                
                ArrayList<byte[]> e1 = dataIndex2011.get(i).queryEvents(
                        th, outlier, attributes2011.get(dataAttributes[i]), "");
                int n1 = dataIndex2011.get(i).nv;
                TopologyTimeSeriesWritable[] tarr1 = new TopologyTimeSeriesWritable[n1];
                for(int j = 0; j < n1; j++) {
                    tarr1[j] = new TopologyTimeSeriesWritable(j, 0, i, e1.get(j),
                            dataIndex2011.get(i).stTime,
                            dataIndex2011.get(i).enTime,
                            dataIndex2011.get(i).getNbPosEvents(j),
                            dataIndex2011.get(i).getNbNegEvents(j),
                            dataIndex2011.get(i).getNbNonEvents(j),
                            outlier);
                }
                
                int temporal = FrameworkUtils.HOUR;
                TimeSeriesStats stats = new TimeSeriesStats();
                
                ArrayList<byte[]> e2 = dataIndex2012.get(i).queryEvents(
                        th, outlier, attributes2012.get(dataAttributes[i]), "");
                int n2 = dataIndex2012.get(i).nv;
                if(n1 != n2) {
                    System.out.println("Something is wrong ...");
                    System.exit(0);
                }
                TopologyTimeSeriesWritable[] tarr2 = new TopologyTimeSeriesWritable[n2];
                for(int j = 0; j < n2; j++) {
                    tarr2[j] = new TopologyTimeSeriesWritable(j, 0, i, e2.get(j),
                            dataIndex2012.get(i).stTime,
                            dataIndex2012.get(i).enTime,
                            dataIndex2012.get(i).getNbPosEvents(j),
                            dataIndex2012.get(i).getNbNegEvents(j),
                            dataIndex2012.get(i).getNbNonEvents(j),
                            outlier);
                    stats.add(CorrelationReducer.getStats(temporal, tarr1[j], tarr2[j], false));
                }
                
                stats.computeScores();
                
                float alignedScore = stats.getRelationshipScore();
                
                System.out.println("Score: " + alignedScore);
                System.out.println("Strength: " + stats.getRelationshipStrength());
    
                double pValue = 0;
                int repetitions = 1000;
                ArrayList<Integer[]> pairs = new ArrayList<Integer[]>();
                
                for (int j = 0; j < repetitions; j++) {
                    pairs.clear();
                    pairs = spatialGraph.generateRandomShift();
                    
                    stats = new TimeSeriesStats();
                    for (int p = 0; p < pairs.size(); p++) {
                        Integer[] pair = pairs.get(p);
                        stats.add(CorrelationReducer.getStats(temporal, 
                                tarr1[pair[0]], tarr2[pair[1]], false));
                    }
                    stats.computeScores();
                    
                    float mcScore = stats.getRelationshipScore();
                    
                    if (alignedScore > 0) {
                        if (mcScore >= alignedScore)
                            pValue += 1;
                    }
                    else {
                        if (mcScore <= alignedScore)
                            pValue += 1;
                    }
                    if (pValue > (alpha*repetitions)) break; // pruning
                }
                
                pValue = pValue/((float)(repetitions));
                
                if (pValue <= alpha)  {
                    System.out.println("p-value: " + pValue);
                } else {
                    System.out.println("p-value: " + pValue + " [not significant]");
                }
            }
        }
    }

    public TopologicalIndex createIndex(HashMap<String, Attribute> attributes,
            String attribute, int spatialRes, int nv, ArrayList<Integer[]> edges) {
        TopologicalIndex index = new TopologicalIndex(
                spatialRes, FrameworkUtils.HOUR, nv);
        Attribute a = attributes.get(attribute);
        index.createIndex(a, edges);
        return index;
    }

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        
        // data set file
        String dataFile = args[0];
        
        // 2D graph file
        String graphFile = args[1];
        
        // 2D polygons
        String polygonsFile = args[2];
        
        // 1D or 2D ?
        boolean is2D = Boolean.parseBoolean(args[3]);
        
        TaxiSyntheticExp pts = new TaxiSyntheticExp();
        pts.run(dataFile, graphFile, polygonsFile, is2D);
    }

}
