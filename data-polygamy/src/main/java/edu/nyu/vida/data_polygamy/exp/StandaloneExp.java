package edu.nyu.vida.data_polygamy.exp;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import edu.nyu.vida.data_polygamy.ct.MergeTrees.TreeType;
import edu.nyu.vida.data_polygamy.ctdata.SpatioTemporalVal;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex;
import edu.nyu.vida.data_polygamy.ctdata.TopologicalIndex.Attribute;
import edu.nyu.vida.data_polygamy.utils.FrameworkUtils;
import edu.nyu.vida.data_polygamy.utils.SpatialGraph;
import edu.nyu.vida.data_polygamy.utils.Utilities;

public class StandaloneExp {

    static String[] dataAttributes = {"count-db_idx"};
    HashSet<String> dataAttributesHashSet = new HashSet<String>(Arrays.asList(dataAttributes)); 
	HashMap<String, Attribute> attributes = new HashMap<String, Attribute>();
	
	HashMap<String, ArrayList<Float>> values = new HashMap<String, ArrayList<Float>>();
	
	TreeType [] types = {TreeType.JoinTree, TreeType.SplitTree};
	static long indexTimes = 0L;
	static long queryTimes = 0L;
	
	void load1DData(String aggregatesFile) {
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
                        int time = Integer.parseInt(s[0]);
                        float value = Float.parseFloat(s[1]);
                        
                        SpatioTemporalVal val = new SpatioTemporalVal(0, time, value);
                        set.add(val);
                        
                        ArrayList<Float> vals = (values.get(attr) == null) ? new ArrayList<Float>() : values.get(attr);
                        vals.add(value);
                        values.put(attr, vals);
                        
                        set.add(val);
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
	
	void load2DData(String aggregateFile) {
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
                        int time = Integer.parseInt(s[0]);
                        float value = Float.parseFloat(s[1]);
                        
                        SpatioTemporalVal val = new SpatioTemporalVal(sid, time, value);
    
                        ArrayList<Float> vals = (values.get(attr) == null) ? new ArrayList<Float>() : values.get(attr);
                        vals.add(value);
                        values.put(attr, vals);
    
                        set.add(val);
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
	
	public TopologicalIndex createIndex(HashMap<String, Attribute> attributes,
            String attribute, int spatialRes, int nv, ArrayList<Integer[]> edges, int noMonths) {
        TopologicalIndex index = new TopologicalIndex(
                spatialRes, FrameworkUtils.HOUR, nv);
        Attribute a = attributes.get(attribute);
        int ct = 0;
        IntSet keys = a.data.keySet();
        IntIterator it = keys.iterator();
        ArrayList<SpatioTemporalVal> arr = new ArrayList<SpatioTemporalVal>();
        while(ct < noMonths) {
            if(!it.hasNext()) {
                Utilities.er("no. of months is greater than what is present");
            }
            int month = it.nextInt();
            arr.addAll(a.data.get(month));
            ct++;
        }
        Collections.sort(arr);
        Attribute na = new Attribute();
        na.data.put(0, arr);
        long st = System.nanoTime();
        index.createIndex(na, edges);
        indexTimes = System.nanoTime() - st;
        return index;
    }
	
	public void test1d(int noMonths, String dataFile) {
        load1DData(dataFile);
        ArrayList<Integer[]> edges = new ArrayList<Integer[]>();
        int spatialRes = FrameworkUtils.CITY;
        int nv = 1;
        
        for(int cc = 0; cc < 3; cc++) {
            TopologicalIndex index = createIndex(attributes, dataAttributes[0], spatialRes, nv, edges, noMonths);
            long st = System.nanoTime();
            ArrayList<byte[]> e1 = index.queryEvents(0.4f, false, attributes.get(dataAttributes[0]), "");
            queryTimes = System.nanoTime() - st;
            e1.clear();
        }
        System.out.println(noMonths + "\t" + indexTimes + "\t" + queryTimes);
    }
	
	public void test2d(int noMonths, String dataFile, String polygonsFile, String graphFile) throws IOException {
        load2DData(dataFile);
        ArrayList<Integer[]> edges = new ArrayList<Integer[]>();
        int spatialRes = FrameworkUtils.NBHD;
        
        SpatialGraph spatialGraph = new SpatialGraph();
        try {
            spatialGraph.init(polygonsFile, graphFile);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(graphFile));
        String [] s = Utilities.splitString(reader.readLine().trim());
        int nv = Integer.parseInt(s[0].trim());
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
        
        for (int cc = 0; cc < 3; cc++) {
            TopologicalIndex index = createIndex(attributes, dataAttributes[0], spatialRes, nv, edges, noMonths);
            long st = System.nanoTime();
            ArrayList<byte[]> e1 = index.queryEvents(0.4f, false, attributes.get(dataAttributes[0]), "");
            queryTimes = System.nanoTime() - st;
            e1.clear();
        }
        
        System.out.println(noMonths + "\t" + indexTimes + "\t" + queryTimes);
    }
	
	public void run(int noMonths, String dataFile, String graphFile, String polygonsFile, boolean is2D) throws IOException {
	    if (!is2D) {
	        test1d(noMonths, dataFile);
	    } else {
	        test2d(noMonths, dataFile, graphFile, polygonsFile);
	    }
    }
	
	public static void main(String[] args) throws IOException {
	    
	    int noMonths = Integer.parseInt(args[0]);
        
        // data set file
        String dataFile = args[1];
        
        // 2D graph file
        String graphFile = args[2];
        
        // 2D polygons
        String polygonsFile = args[3];
        
        // 1D or 2D ?
        boolean is2D = Boolean.parseBoolean(args[4]);
        
        StandaloneExp exp = new StandaloneExp();
        exp.run(noMonths, dataFile, graphFile, polygonsFile, is2D);
    }
}
