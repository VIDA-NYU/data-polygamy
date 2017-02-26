/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.nyu.vida.data_polygamy.utils.Utilities;

public class SpatialGraph {
    
    private ArrayList<ArrayList<Integer>> adjacencyList = new ArrayList<ArrayList<Integer>>();
    private ArrayList<ArrayList<Integer>> bfsLevelArray = new ArrayList<ArrayList<Integer>>();
    private Random random = new Random();
    private int mainOriginNode = 0;
    private int nbNodes = 0;

    public void init(String nodeFile, String edgeFile) throws IOException {
    	
        BufferedReader buff = new BufferedReader(new FileReader(nodeFile));
        
        String line = buff.readLine();
        HashSet<Integer> set = new HashSet<Integer>();
        while (line != null) {
            //int id = Integer.parseInt(line);
        	// assuming that neighborhoods and zip codes are read in order
        	//  and the same ids are present in the edges file
        	int id = Integer.parseInt(line.trim());
        	if(!set.contains(id)) {
        		adjacencyList.add(new ArrayList<Integer>());	
        		set.add(id);
        	}
            
            buff.readLine();
            int nbPoints = Integer.parseInt(buff.readLine());
            for (int i = 0; i < nbPoints; i++)
                buff.readLine();
            
            line = buff.readLine();
        }
        buff.close();
        
        nbNodes = adjacencyList.size();
        
        // reading edges
        buff = new BufferedReader(new FileReader(edgeFile));
        line = buff.readLine();
        line = buff.readLine();
        String[] values;
        ArrayList<Integer> elem;
        Integer id_1, id_2;
        while (line != null) {
            values = line.replace("\n", "").split(" ");
            id_1 = Integer.parseInt(values[0]);
            id_2 = Integer.parseInt(values[1]);
            
            elem = adjacencyList.get(id_1);
            elem.add(id_2);
            adjacencyList.set(id_1, elem);
            
            elem = adjacencyList.get(id_2);
            elem.add(id_1);
            adjacencyList.set(id_2, elem);
            
            line = buff.readLine();
        }
        buff.close();
        
        // main BFS - choose random node
        mainOriginNode = random.nextInt(nbNodes); 
        bfs(mainOriginNode);
        
    }

    
    public void init(int resolution, Configuration conf) throws IOException {
    	
    	String bucket = conf.get("bucket", "");
    	Path edgesPath = null;
    	FileSystem fs = null;
        
        // reading edges
    	switch (resolution) {
    	
    	case FrameworkUtils.NBHD:
    	    edgesPath = new Path(bucket + "neighborhood-graph");
    	    break;
    	case FrameworkUtils.ZIP:
    	    edgesPath = new Path(bucket + "zipcode-graph");
    	    break;
    	case FrameworkUtils.BLOCK:
            edgesPath = new Path(bucket + "block-graph");
            break;
        default:
            edgesPath = new Path(bucket + "zipcode-graph");
            break;
    	}
        
        if (!bucket.equals(""))
            fs = FileSystem.get(edgesPath.toUri(), conf);
        else
            fs = FileSystem.get(new Configuration());
        
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(edgesPath)));
        String line = buff.readLine();
        
        String [] s = Utilities.splitString(line.trim());
        nbNodes = Integer.parseInt(s[0].trim());
        
        for (int i = 0; i < nbNodes; i++) {
            // assuming that neighborhoods and zip codes are read in order
            //  and the same ids are present in the edges file
            adjacencyList.add(new ArrayList<Integer>());
        }
        
        line = buff.readLine();
        String[] values;
        ArrayList<Integer> elem;
        Integer id_1, id_2;
        while (line != null) {
            values = line.replace("\n", "").split(" ");
            id_1 = Integer.parseInt(values[0]);
            id_2 = Integer.parseInt(values[1]);
            
            elem = adjacencyList.get(id_1);
            elem.add(id_2);
            adjacencyList.set(id_1, elem);
            
            elem = adjacencyList.get(id_2);
            elem.add(id_1);
            adjacencyList.set(id_2, elem);
            
            line = buff.readLine();
        }
        buff.close();
        fs.close();
        
        // main BFS - choose random node
        mainOriginNode = random.nextInt(nbNodes); 
        bfs(mainOriginNode);
        
    }
    
    public void init(SpatialGraph graph) {
        this.adjacencyList = new ArrayList<ArrayList<Integer>>(graph.getAdjacencyList());
        this.bfsLevelArray = new ArrayList<ArrayList<Integer>>(graph.getBfsLevelArray());
        this.nbNodes = graph.nbNodes();
        this.mainOriginNode = graph.getMainOriginNode();
    }
    
    public int nbNodes() {
        return nbNodes;
    }
    
    public ArrayList<ArrayList<Integer>> getAdjacencyList() {
        return adjacencyList;
    }

    public ArrayList<ArrayList<Integer>> getBfsLevelArray() {
        return bfsLevelArray;
    }

    public int getMainOriginNode() {
        return mainOriginNode;
    }
    
    private void bfs(int node) {
        
        Queue<Integer> queue = new LinkedList<Integer>();
        HashSet<Integer> seen = new HashSet<Integer>();
        HashMap<Integer,Integer> level = new HashMap<Integer,Integer>();
        
        queue.add(node);
        level.put(node, 0);
        seen.add(node);
        ArrayList<Integer> elem;
        while (!queue.isEmpty()) {
            // current node
            int currentNode = queue.poll();
            
            // level of current node
            int nodeLevel = level.get(currentNode);
            
            // update the array of levels
            if (bfsLevelArray.size() == nodeLevel)
                bfsLevelArray.add(new ArrayList<Integer>());
            elem = bfsLevelArray.get(nodeLevel);
            elem.add(currentNode);
            bfsLevelArray.set(nodeLevel, elem);
            
            for (Integer neighbor: adjacencyList.get(currentNode)) {
                if (!seen.contains(neighbor)) {
                    queue.add(neighbor);
                    seen.add(neighbor);
                    level.put(neighbor, nodeLevel + 1);
                }
            }
        }
        
    }
    
    private ArrayList<ArrayList<Integer>> randomBFS() {
        
        int origin = random.nextInt(nbNodes);
        
        Queue<Integer> queue = new LinkedList<Integer>();
        int[] seen = new int[nbNodes];
        
        ArrayList<ArrayList<Integer>> randomBFSLevelArray = new ArrayList<ArrayList<Integer>>();
        HashMap<Integer,Integer> level = new HashMap<Integer,Integer>();
        
        int[] leftNodes = new int[nbNodes];
        for (int i = 0; i < nbNodes; i++) {
            seen[i] = 0;
            leftNodes[i] = 1;
        }
        
        // running BFS
        
        queue.add(origin);
        level.put(origin, 0);
        seen[origin] = 1;
        ArrayList<Integer> elem;
        int currentLevel = -1;
        int count = 0;
        while (!queue.isEmpty()) {
            // current node
            int currentNode = queue.poll();
            
            // node level
            int nodeLevel = level.get(currentNode);
            
            // current level
            if (nodeLevel > currentLevel) {
                currentLevel = nodeLevel;
                if (bfsLevelArray.size() == currentLevel)
                    break;
                count = 1;
                randomBFSLevelArray.add(new ArrayList<Integer>());
            } else {
                // guaranteeing that the random BFS will have the same structure as the main one
                count++;
                if (count > bfsLevelArray.get(currentLevel).size())
                    continue;
            }
            
            elem = randomBFSLevelArray.get(currentLevel);
            elem.add(currentNode);
            randomBFSLevelArray.set(currentLevel, elem);
            leftNodes[currentNode] = 0;
            
            int nextLevel = nodeLevel + 1;
            if (bfsLevelArray.size() == nextLevel)
                continue;
            int sizeOriginalNextLevel = bfsLevelArray.get(nextLevel).size();
            int nodesCount = 0;
            for (Integer neighbor: adjacencyList.get(currentNode)) {
                if (seen[neighbor] == 0) {
                    if (++nodesCount > sizeOriginalNextLevel)
                        break;
                    queue.add(neighbor);
                    seen[neighbor] = 1;
                    level.put(neighbor, nextLevel);
                }
            }
        }
        
        // filling the gaps
        
        for (int i = 0; i < bfsLevelArray.size(); i++) {
            if (i+1 > randomBFSLevelArray.size())
                randomBFSLevelArray.add(new ArrayList<Integer>());
            
            elem = randomBFSLevelArray.get(i);
            int sizeOriginalElem = bfsLevelArray.get(i).size();
            int diff = sizeOriginalElem - elem.size();
            for (int j = 0; j < diff; j++) {
                // start looking for neighbors in the previous level,
                // and keep going down in the tree to find neighbors
                int bfsIndex = i - 1;
                Integer node = 0;
                boolean found = false;
                while (!found) {
                    for (Integer nbhd: randomBFSLevelArray.get(bfsIndex)) {
                        for (Integer neighbor: adjacencyList.get(nbhd)) {
                            if (leftNodes[neighbor] == 1) {
                                leftNodes[neighbor] = 0;
                                node = neighbor;
                                found = true;
                                break;
                            }
                        }
                        if (found) break;
                    }
                    bfsIndex++;
                    if (bfsIndex == randomBFSLevelArray.size()) {
                        bfsIndex = 0;
                    }
                }
                elem.add(node);
            }
            randomBFSLevelArray.set(i, elem);
        }
        
        return randomBFSLevelArray;
        
    }
    
    public ArrayList<Integer[]> generateRandomShift() {
        
        ArrayList<Integer[]> result = new ArrayList<Integer[]>();
        
        ArrayList<ArrayList<Integer>> randomBFS = this.randomBFS();
        ArrayList<Integer> elem;
        ArrayList<Integer> randomElem;
        for (int i = 0; i < bfsLevelArray.size(); i++) {
            elem = bfsLevelArray.get(i);
            randomElem = randomBFS.get(i);
            if (elem.size() != randomElem.size())
                System.out.println("Something is wrong...");
            for (int j = 0; j < elem.size(); j++) {
                Integer[] pair = new Integer[2];
                pair[0] = elem.get(j);
                pair[1] = randomElem.get(j);
                result.add(pair);
            }
        }
        
        return result;
    }
    
}
