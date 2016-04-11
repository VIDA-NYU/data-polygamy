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
import java.util.Random;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;

import edu.nyu.vida.data_polygamy.relationship_computation.CorrelationReducer;
import edu.nyu.vida.data_polygamy.utils.SpatialGraph;
import edu.nyu.vida.data_polygamy.ctdata.SpatioTemporalVal;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex.Attribute;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TimeSeriesStats;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TimeSeriesWritable;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils.TopologyTimeSeriesWritable;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class NoiseExp {
    
    static String[] dataAttributes = {"count-db_idx", "unique-medallion_id",
            "avg-miles", "avg-fare"};
    HashSet<String> dataAttributesHashSet = new HashSet<String>(Arrays.asList(dataAttributes)); 
    HashMap<String, Attribute> attributes = new HashMap<String, Attribute>();
    Random r = new java.util.Random();
    float alpha = 0.05f;
    
    HashMap<String, ArrayList<Float>> values = new HashMap<String, ArrayList<Float>>();
    HashMap<String, Double> iqr = new HashMap<String, Double>();
    
    void load1DData(String aggregatesFile, int year) {
        String[] s = null;
        try {
            BufferedReader buf = new BufferedReader(new FileReader(aggregatesFile));
            s = Utilities.getLine(buf, ",");
            while (true) {
                if (s == null) {
                    break;
                }
                String attr = Utilities.splitString(s[0], ":")[1].trim();
                //System.out.println("Attribute: " + attr);
                Attribute a = new Attribute();
                a.nodeSet.add(0);
                s = Utilities.getLine(buf, ",");
                if(s != null && s.length > 0 && s[0].toLowerCase().startsWith("spatial")) {
                    s = Utilities.getLine(buf, ",");
                }
                if(s == null || s.length == 0) {
                    System.out.println("Empty: ---------------------- " + attr);
                }
                while (s != null && s.length > 0) {
                    int month = Integer.parseInt(Utilities.splitString(s[0], ":")[1].trim());
                    s = Utilities.getLine(buf, ",");
                    HashSet<SpatioTemporalVal> set = new HashSet<SpatioTemporalVal>();
                    while (s != null && s.length == 2) {
                        if (month/100 == year) {
                            int time = Integer.parseInt(s[0]);
                            float value = Float.parseFloat(s[1]);
                            
                            SpatioTemporalVal val = new SpatioTemporalVal(0, time, value);
                            set.add(val);
                            
                            ArrayList<Float> vals = (values.get(attr) == null) ? new ArrayList<Float>() : values.get(attr);
                            vals.add(value);
                            values.put(attr, vals);
                            
                            set.add(val);
                        }
                        s = Utilities.getLine(buf, ",");
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
                s = Utilities.getLine(buf, ",");
            }
            buf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    void load2DData(String aggregateFile, String graphFile, int year) {
        String[] s = null;
        IntOpenHashSet nodeSet = new IntOpenHashSet();
        try {
            BufferedReader buf = new BufferedReader(new FileReader(aggregateFile));
            s = Utilities.getLine(buf, ",");
            //System.out.println(s[0]);
            while (true) {
                if (s == null) {
                    break;
                }
                String attr = Utilities.splitString(s[0], ":")[1].trim();
                Attribute a = attributes.get(attr);
                if(a == null) {
                    a = new Attribute();
                    attributes.put(attr, a);
                }
                s = Utilities.getLine(buf, ":");
                int sid = Integer.parseInt(s[1].trim());
                nodeSet.add(sid);
                s = Utilities.getLine(buf, ",");
                while (s != null && s.length > 0) {
                    int month = Integer.parseInt(Utilities.splitString(s[0], ":")[1].trim());
                    s = Utilities.getLine(buf, ",");
                    HashSet<SpatioTemporalVal> set = new HashSet<SpatioTemporalVal>();
                    while (s != null && s.length == 2) {
                        if (month/100 == year) {
                            int time = Integer.parseInt(s[0]);
                            float value = Float.parseFloat(s[1]);
                            
                            SpatioTemporalVal val = new SpatioTemporalVal(sid, time, value);

                            ArrayList<Float> vals = (values.get(attr) == null) ? new ArrayList<Float>() : values.get(attr);
                            vals.add(value);
                            values.put(attr, vals);

                            set.add(val);
                        }
                        s = Utilities.getLine(buf, ",");
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
                s = Utilities.getLine(buf, ",");
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
    }
    
    public void noiseExp(String dataFile, String graphFile,
            String polygonsFile, boolean is2D) throws IOException {
        
        ArrayList<TopologicalIndex> mainDataIndex = new ArrayList<>();
        
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
        
        //long st = System.currentTimeMillis();
        if (!is2D) {
            this.load1DData(dataFile, 2011);
        } else {
            this.load2DData(dataFile, graphFile, 2011);
        }
        //long en = System.currentTimeMillis();
        //System.out.println("time to load data: " + (en - st));
        for(int i = 0; i < dataAttributes.length; i++) {
            //System.out.println("Creating index for data " + dataAttributes[i]);
            TopologicalIndex index = this.createIndex(attributes, dataAttributes[i], spatialRes, nv, edges);
            mainDataIndex.add(index);
            iqr.put(dataAttributes[i], getIQR(values.get(dataAttributes[i])));
        }
        
        for (int magn = 1; magn <= 10000; magn++) {
            
            System.out.println("Amplitude: " + magn);
            ArrayList<TopologicalIndex> noiseDataIndex = new ArrayList<>();
            
            // adding noise to data
            HashMap<String, Attribute> newAttributes = addNoise((double)magn);
            for(int i = 0; i < dataAttributes.length; i++) {
                //System.out.println("Creating index for data " + dataAttributes[i]);
                TopologicalIndex index = this.createIndex(newAttributes, dataAttributes[i], spatialRes, nv, edges);
                noiseDataIndex.add(index);
            }
            
            boolean outlier = false;
            float th = 0.90f;
            
            if (!is2D) {
                
                for(int i = 0; i < dataAttributes.length; i++) {
                    System.out.println("Attribute: " + dataAttributes[i]);
                    
                    ArrayList<byte[]> e1 = mainDataIndex.get(i).queryEvents(
                            th, outlier, attributes.get(dataAttributes[i]), "");
                    ArrayList<byte[]> e2 = noiseDataIndex.get(i).queryEvents(
                            th, outlier, newAttributes.get(dataAttributes[i]), "");
                    
                    TopologyTimeSeriesWritable t1 = 
                            new TopologyTimeSeriesWritable(0, 0, i, e1.get(0),
                                    mainDataIndex.get(i).stTime,
                                    mainDataIndex.get(i).enTime,
                                    mainDataIndex.get(i).getNbPosEvents(0),
                                    mainDataIndex.get(i).getNbNegEvents(0),
                                    mainDataIndex.get(i).getNbNonEvents(0),
                                    outlier);
                    
                    TopologyTimeSeriesWritable t2 = 
                            new TopologyTimeSeriesWritable(0, 0, i, e2.get(0),
                                    noiseDataIndex.get(i).stTime,
                                    noiseDataIndex.get(i).enTime,
                                    noiseDataIndex.get(i).getNbPosEvents(0),
                                    noiseDataIndex.get(i).getNbNegEvents(0),
                                    noiseDataIndex.get(i).getNbNonEvents(0),
                                    outlier);
                    
                    int temporal = FrameworkUtils.HOUR;
                    TimeSeriesStats stats = CorrelationReducer.getStats(temporal, t1, t2, false);
                    
                    stats.computeScores();
                    if (!stats.isIntersect())
                        return;
                    
                    float alignedScore = stats.getRelationshipScore();
                    
                    System.out.println("Score: " + alignedScore);
                    System.out.println("Strength: " + stats.getRelationshipStrength());
                    //System.out.println(stats.getMatchEvents() + " " + stats.getMatchPosEvents() + " " + stats.getMatchNegEvents());
                    //System.out.println(t1.getNbNegEvents() + " " + t1.getNbPosEvents());
                    //System.out.println(t2.getNbNegEvents() + " " + t2.getNbPosEvents());
    
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
                    
                    ArrayList<byte[]> e1 = mainDataIndex.get(i).queryEvents(
                            th, outlier, attributes.get(dataAttributes[i]), "");
                    int n1 = mainDataIndex.get(i).nv;
                    TopologyTimeSeriesWritable[] tarr1 = new TopologyTimeSeriesWritable[n1];
                    for(int j = 0; j < n1; j++) {
                        tarr1[j] = new TopologyTimeSeriesWritable(j, 0, i, e1.get(j),
                                mainDataIndex.get(i).stTime,
                                mainDataIndex.get(i).enTime,
                                mainDataIndex.get(i).getNbPosEvents(j),
                                mainDataIndex.get(i).getNbNegEvents(j),
                                mainDataIndex.get(i).getNbNonEvents(j),
                                outlier);
                    }
                    
                    int temporal = FrameworkUtils.HOUR;
                    TimeSeriesStats stats = new TimeSeriesStats();
                    
                    ArrayList<byte[]> e2 = noiseDataIndex.get(i).queryEvents(
                            th, outlier, newAttributes.get(dataAttributes[i]), "");
                    int n2 = noiseDataIndex.get(i).nv;
                    if(n1 != n2) {
                        System.out.println("Something is wrong ...");
                        System.exit(0);
                    }
                    TopologyTimeSeriesWritable[] tarr2 = new TopologyTimeSeriesWritable[n2];
                    for(int j = 0; j < n2; j++) {
                        tarr2[j] = new TopologyTimeSeriesWritable(j, 0, i, e2.get(j),
                                noiseDataIndex.get(i).stTime,
                                noiseDataIndex.get(i).enTime,
                                noiseDataIndex.get(i).getNbPosEvents(j),
                                noiseDataIndex.get(i).getNbNegEvents(j),
                                noiseDataIndex.get(i).getNbNonEvents(j),
                                outlier);
                        stats.add(CorrelationReducer.getStats(temporal, tarr1[j], tarr2[j], false));
                    }
                    
                    stats.computeScores();
                    
                    float alignedScore = stats.getRelationshipScore();
                    
                    System.out.println("Score: " + alignedScore);
                    System.out.println("Strength: " + stats.getRelationshipStrength());
                    //System.out.println(stats.getMatchEvents() + " " + stats.getMatchPosEvents() + " " + stats.getMatchNegEvents());
                    //System.out.println(nn1 + " " + np1);
                    //System.out.println(nn2 + " " + np2);

                    double pValue = 0;
                    int repetitions = 1000;
                    ArrayList<Integer[]> pairs = new ArrayList<Integer[]>();
                    
                    for (int j = 0; j < repetitions; j++) {
                        pairs.clear();
                        pairs = spatialGraph.generateRandomShift();
                        
                        stats = new TimeSeriesStats();
                        for (int p = 0; p < pairs.size(); p++) {
                            Integer[] pair = pairs.get(p);
                            stats.add(CorrelationReducer.getStats(temporal, tarr1[pair[0]],
                                    tarr2[pair[1]], false));
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
    }
    
    public HashMap<String, Attribute> addNoise(double increase) {
        HashMap<String, Attribute> newAttributes = new HashMap<String, Attribute>();
        
        for (String att : attributes.keySet()) {
            if (!(dataAttributesHashSet.contains(att))) continue;
            
            Attribute attribute = attributes.get(att);
            
            double increment = iqr.get(att) / 10000.0;
            
            for (int month : attribute.data.keySet()) {
                ArrayList<SpatioTemporalVal> arr = attribute.data.get(month);
                
                for (int j = 0; j < arr.size(); j++) {
                    SpatioTemporalVal val = arr.get(j);
                    float noiseIncrement = (float) (r.nextGaussian()*increment*increase);
                    val.setVal(val.getVal() + noiseIncrement);
                    arr.set(j, val);
                }
                
                attribute.data.put(month, arr);
            }
            newAttributes.put(att, attribute);
        }
        
        return newAttributes;
    }
    
    public TopologicalIndex createIndex(HashMap<String, Attribute> attributes,
            String attribute, int spatialRes, int nv, ArrayList<Integer[]> edges) {
        TopologicalIndex index = new TopologicalIndex(
                spatialRes, FrameworkUtils.HOUR, nv);
        Attribute a = attributes.get(attribute);
        index.createIndex(a, edges);
        return index;
    }
    
    public static TimeSeriesStats getStats(TimeSeriesWritable timeSeries1,
            TimeSeriesWritable timeSeries2, boolean temporalPermutationTest) {
        
        TimeSeriesStats output = new TimeSeriesStats();
        
        if ((timeSeries1 == null) || (timeSeries2 == null))
            return output;
        
        // detecting intersection
        
        long start1 = timeSeries1.getStart();
        long end1 = timeSeries1.getEnd();
        long start2 = timeSeries2.getStart();
        long end2 = timeSeries2.getEnd();
        
        if (((end1 < start2) && (start1 < start2)) || ((end1 > end2) && (start1 > end2)))
            return output;
        
        output.setIntersect(true);
        
        DateTime start1Obj = new DateTime(start1*1000, DateTimeZone.UTC);
        DateTime end1Obj = new DateTime(end1*1000, DateTimeZone.UTC);
        DateTime start2Obj = new DateTime(start2*1000, DateTimeZone.UTC);
        DateTime end2Obj = new DateTime(end2*1000, DateTimeZone.UTC);
        
        byte[] eventTimeSeries1 = timeSeries1.getTimeSeries();
        byte[] eventTimeSeries2 = timeSeries2.getTimeSeries();
        
        int startRange = 0;
        int endRange = 0;
        
        startRange = (start1 > start2) ? Hours.hoursBetween(start2Obj, start1Obj).getHours() : Hours.hoursBetween(start1Obj, start2Obj)
                .getHours();
        endRange = (end1 > end2) ? Hours.hoursBetween(end2Obj, end1Obj).getHours() : Hours.hoursBetween(end1Obj, end2Obj).getHours();
        
        int indexStart1 = (start2 > start1) ? startRange : 0;
        int indexStart2 = (start2 > start1) ? 0 : startRange;
        int indexEnd1 = (end2 > end1) ? eventTimeSeries1.length : eventTimeSeries1.length - endRange;
        int indexEnd2 = (end2 > end1) ? eventTimeSeries2.length - endRange : eventTimeSeries2.length;
        
        byte[] timeSeries1Int = Arrays.copyOfRange(eventTimeSeries1, indexStart1, indexEnd1);
        byte[] timeSeries2Int = Arrays.copyOfRange(eventTimeSeries2, indexStart2, indexEnd2);
        
        if (timeSeries1Int.length != timeSeries2Int.length) {
            System.out.println("Something went wrong... Different sizes");
            System.exit(-1);
        }
        if(timeSeries1Int.length == 0) {
            return output;
        }
        
        int nMatchEvents = 0;
        int nMatchPosEvents = 0;
        int nMatchNegEvents = 0;
        int nPosFirstNonSecond = 0;
        int nNegFirstNonSecond = 0;
        int nNonFirstPosSecond = 0;
        int nNonFirstNegSecond = 0;
        
        int indexD1 = (temporalPermutationTest) ? new Random().nextInt(timeSeries1Int.length) : 0;
        int indexD2 = (temporalPermutationTest) ? new Random().nextInt(timeSeries2Int.length) : 0;
        for (int i = 0; i < timeSeries1Int.length; i++) {
            int j = (indexD1 + i) % timeSeries1Int.length;
            int k = (indexD2 + i) % timeSeries2Int.length;            
            byte result = (byte) (timeSeries1Int[j] | timeSeries2Int[k]);
            
            switch(result) {
            case FrameworkUtils.nonEventsMatch: // both non events
                // do nothing
                break;
            case FrameworkUtils.posEventsMatch: // both positive
                nMatchEvents++;
                nMatchPosEvents++;
                break;
            case FrameworkUtils.nonEventPosEventMatch: // one positive, one non-event
                if (timeSeries1Int[j] == FrameworkUtils.positiveEvent)
                    nPosFirstNonSecond++;
                else
                    nNonFirstPosSecond++;
                break;
            case FrameworkUtils.negEventsMatch: // both negative
                nMatchEvents++;
                nMatchPosEvents++;
                break;
            case FrameworkUtils.nonEventNegEventMatch: // one negative, one non-event
                if (timeSeries1Int[j] == FrameworkUtils.negativeEvent)
                    nNegFirstNonSecond++;
                else
                    nNonFirstNegSecond++;
                break;
            case FrameworkUtils.negEventPosEventMatch: // one negative, one positive
                nMatchEvents++;
                nMatchNegEvents++;
                break;
            default:
                System.out.println("Something went wrong... Wrong case");
                System.exit(-1);
            }
        }
        
        output.setParameters(
                nMatchEvents,
                nMatchPosEvents,
                nMatchNegEvents,
                nPosFirstNonSecond,
                nNegFirstNonSecond,
                nNonFirstPosSecond,
                nNonFirstNegSecond);
        
        return output;
    }
    
    public double getIQR(ArrayList<Float> arrayList) {
        double[] vals = new double[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            vals[i] = (double)arrayList.get(i);
        }
        DescriptiveStatistics ds = new DescriptiveStatistics(vals);
        double fq = ds.getPercentile(25);
        double tq = ds.getPercentile(75);
        return (tq - fq);
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
        
        NoiseExp pts = new NoiseExp();
        pts.noiseExp(dataFile, graphFile, polygonsFile, is2D);
    }

}
