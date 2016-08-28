/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.exp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.nyu.vida.data_polygamy.utils.Utilities;

public class RelationGraph {
	
	public String perm = "";
	public String events = "";
	public String temporalRes = "";
	public String spatialRes = "";
	
	String aggregatesPatternStr = "([a-zA-Z0-9]+[\\-]?[a-zA-Z0-9]+){1}\\-[a-z]+\\-[a-z]+\\.[a-z]+";
	String relationshipPatternStr = "([a-zA-Z0-9]+[\\-]?[a-zA-Z0-9]+){1}\\-([a-zA-Z0-9]+" +
	        "[\\-]?[a-zA-Z0-9]+){1}\\-([a-z]+)\\-([a-z]+)\\-[a-z]+\\-[a-z]+";
	Pattern aggregatesPattern = Pattern.compile(aggregatesPatternStr);
	Pattern relationshipPattern = Pattern.compile(relationshipPatternStr);
	
	public class DataSet {
		public int id;
		public String name;
		public int nAttributes = 0;
		public int nFiles = 0;
		public boolean hasData = false;
		
		public HashSet<Integer> attributes = new HashSet<Integer>();
		public HashSet<Integer> adjacent = new HashSet<Integer>();
	}
	
	public class Attribute {
		public int id;
		public String dataset;
		public String name;
		
		public HashSet<Integer> adjacent = new HashSet<Integer>();
	}
	
	public class Edge {
		int a1;
		int a2;
		
		int d1;
		int d2;
		
		double f1;
		double kappa;
		double pvalue;
	}
	
	HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
	HashMap<String, Integer> attributeMap = new HashMap<String, Integer>();
	
	ArrayList<DataSet> datasets = new ArrayList<DataSet>();
	ArrayList<Attribute> attributes = new ArrayList<Attribute>();
	
	ArrayList<Edge> edges = new ArrayList<Edge>();
	
	public void createGraph(String dataFolder, String metadataFolder, final String events, final String perm,
			String temporalRes, String spatialRes) throws IOException {
		
		this.perm = perm;
		this.events = events;
		this.temporalRes = temporalRes;
		this.spatialRes = spatialRes;
		
		File fol = new File(dataFolder);
		
		FilenameFilter fileNameFilter = new FilenameFilter() {
            
            @Override
            public boolean accept(File dir, String name) {
                if (name.contains(".crc"))
                    return false;
                if (name.contains(perm) && name.contains(events))
                    return true;
                return false;
            }
        };
		
		File[] allFiles = fol.listFiles(fileNameFilter);
		for(File file: allFiles) {
            String fileName = file.getName().replace("gas-prices", "gasprices");
            System.err.println(fileName);
            
            Matcher m = relationshipPattern.matcher(fileName);
            if (!m.find()) continue;
            
            String ds1 = m.group(1);
            String ds2 = m.group(2);
            
            String temporalResolution = m.group(3);
            String spatialResolution = m.group(4);
            
            DataSet d1 = getDataSet(ds1);
            DataSet d2 = getDataSet(ds2);
            
            d1.nFiles++;
            d2.nFiles++;
            
            if (temporalResolution.equals(temporalRes) &&
                    spatialResolution.equals(spatialRes)) {
                d1.hasData = true;
                d2.hasData = true;
                
                BufferedReader reader = new BufferedReader(new FileReader(file));
                while(readPair(reader,d1,d2));
                reader.close();
            }
        }
		
		// Metadata.
		
		FilenameFilter metadataFilter = new FilenameFilter() {
		    
            @Override
            public boolean accept(File dir, String name) {
               if (name.contains(".aggregates"))
                  return true;
               return false;
            }
        };
		
        fol = new File(metadataFolder);
        File[] metadataFiles = fol.listFiles(metadataFilter);
        for(File file: metadataFiles) {
            String fileName = file.getName().replace("gas-prices", "gasprices");
            
            Matcher m = aggregatesPattern.matcher(fileName);
            if (!m.find()) continue;
            String dataset = m.group(1);
            
            if (!datasetExists(dataset))
                continue;
            DataSet d = getDataSet(dataset);
            
            System.err.println(dataset);
            
            BufferedReader reader = new BufferedReader(new FileReader(file));
            reader.readLine();
            int count = Integer.parseInt(reader.readLine());
            d.nAttributes = count;
            reader.close();
         }
	}

	private String replace(String string) {
		return string.replace(' ', '_').replace('.', '_').replace('[', '_').replace(']', '_').replace('-', '_');
	}

	private boolean readPair(BufferedReader reader, DataSet d1, DataSet d2) throws IOException {
		String [] a = Utilities.getLine(reader, "\t");
		
		if(a == null) {
			return false;
		}
		
		String [] attributes = a[0].split(",");
		String [] values = a[1].split(",");
		
		Edge e = new Edge();
		e.kappa = Double.parseDouble(values[0]);
		e.f1 = Double.parseDouble(values[1]);
		
		double pValueLine = Double.parseDouble(values[2]);
		e.pvalue = pValueLine;
		
		Attribute a1 = getAttribute(d1,attributes[0]);
		Attribute a2 = getAttribute(d2,attributes[1]);
			
		e.a1 = a1.id;
		e.a2 = a2.id;
		
		e.d1 = d1.id;
		e.d2 = d2.id;
			
		int ein = edges.size();
		edges.add(e);
		a1.adjacent.add(ein);
		a2.adjacent.add(ein);
		
		d1.adjacent.add(d2.id);
		d2.adjacent.add(d1.id);
		
		return true;
	}

	private Attribute getAttribute(DataSet d, String aName) {
		String s = d.name + "." + aName;
		Integer ain = attributeMap.get(s);
		if(ain == null) {
			Attribute a = new Attribute();
			a.name = aName;
			a.dataset = d.name;
			ain = attributes.size();
			a.id = ain;
			
			attributeMap.put(s,ain);
			attributes.add(a);
			d.attributes.add(ain);
		}
		return attributes.get(ain);
	}
	
	private boolean datasetExists(String name) {
	    if (dataMap.get(name) == null)
	        return false;
	    return true;
	}

	private DataSet getDataSet(String name) {
		Integer din = dataMap.get(name);
		if(din == null) {
			DataSet d = new DataSet();
			d.name = name;
			din = datasets.size();
			d.id = din;
			
			dataMap.put(name,din);
			datasets.add(d);
		}
		return datasets.get(din);
	}

	public void printMetaGraph(String outputDir, String score, String strength, String pValue) throws IOException {
	    
	    int totalNDatasets = dataMap.size();
	    if (totalNDatasets < 2) return;
	    
	    double kTh = Double.parseDouble(score);
        double f1Th = Double.parseDouble(strength);
        double pValueDouble = Double.parseDouble(pValue);
        
        class DataSetComparator implements Comparator<DataSet> {
            @Override
            public int compare(DataSet o1, DataSet o2) {
                return o1.nAttributes - o2.nAttributes;
            }
        }
        
        ArrayList<DataSet> sortedList = new ArrayList<DataSet>(datasets);
        Collections.sort(sortedList, new DataSetComparator());
        
        for (int nDatasets = 2; nDatasets < totalNDatasets + 1; ++nDatasets) {
        
            HashSet<DataSet> finalDataSets = new HashSet<DataSet>();
            HashSet<String> finalDataSetsIds = new HashSet<String>();
            for (DataSet d : sortedList) {
                if (!d.hasData) continue;
                finalDataSets.add(d);
                finalDataSetsIds.add(d.name);
                System.out.println("Dataset: " + d.name);
                if (finalDataSets.size() == nDatasets) break;
            }
            
            System.out.println("");
            
            if (finalDataSets.size() != nDatasets) continue;
            
            int maxEdges = 0;
            int nAttributes = 0;
            int numberSignificant = 0;
            
            for (DataSet d1 : finalDataSets) {
                nAttributes += d1.nAttributes;
                for (DataSet d2 : finalDataSets) {
                    if (d1.id == d2.id) continue;
                    maxEdges += d1.nAttributes * d2.nAttributes;
                }
            }
            maxEdges /= 2;
            
            ArrayList<Edge> finalEdges = new ArrayList<Edge>();
            for(Edge e: edges) {
                Attribute a1 = attributes.get(e.a1);
                Attribute a2 = attributes.get(e.a2);
                
                if (!finalDataSetsIds.contains(a1.dataset)) continue;
                if (!finalDataSetsIds.contains(a2.dataset)) continue;
                
                if(e.pvalue <= pValueDouble) {
                    
                    numberSignificant++;
                    
                    if ((e.f1 >= f1Th) && (Math.abs(e.kappa) >= kTh)) {
                        finalEdges.add(e);
                    }
                }
            }
    	    
    	    File outputFile = new File(outputDir, kTh + "-" + this.events + "-" + this.perm +
    	            "-" + this.temporalRes + "-" + this.spatialRes + "-" + nDatasets + ".out");
    	    
    	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)));
    	    String result = "";
    	    
    	    result += "datasets: " + nDatasets + "\n";
    	    result += "attributes: " + nAttributes + "\n";
    	    result += "max edges: " + maxEdges + "\n";
    	    result += "significant edges: " + numberSignificant + "\n";
    	    result += "  score >= " + kTh + ": " + finalEdges.size() + "\n\n";
    	    
    	    result += "graph relationship {\n";
    		String [] color = {"red","blue"};
    		
    		for(Edge e: finalEdges) {
    			Attribute a1 = attributes.get(e.a1);
    			Attribute a2 = attributes.get(e.a2);
    			int c = 0;
    			if(e.kappa < 0) {
    				c = 1;
    			}
    			result += replace(a1.dataset) + "_" + replace(a1.name) + " -- " + replace(a2.dataset) + "_" + replace(a2.name) +
    			        " (score=" + e.kappa + ",strength=" + e.f1 + ") [color=" + color[c] + "]" + ";\n";
    		}
    		result += "}\n";
    		
    		bw.write(result);
    		bw.close();
        }
	}
}
