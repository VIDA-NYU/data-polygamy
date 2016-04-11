/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ctdata;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class InputProperties {
	
	public static final String scalarFolder = "scalar/";
	public static final String featureFolder = "full/";
	public static final String simplifyFolder = "simplified/";
	
	private Properties props;
	
	public String folder;
	public String graphFile;
	public String scalarFile;
	
	public int modelCt;
	
	public int persistence;
	public int noFeatures;
	public int sigFeatures;
	
	public float th;
	
	public String opFol;
	
	public InputProperties() throws FileNotFoundException, IOException {
		props = new Properties();
		props.load(new FileInputStream("input.properties"));
		graphFile = props.getProperty("graphFile");
		opFol = props.getProperty("outputFol");
		
		scalarFile = props.getProperty("scalarFile");
		modelCt = Integer.parseInt(props.getProperty("models"));
		
		persistence = Integer.parseInt(props.getProperty("simplification"));
		noFeatures = Integer.parseInt(props.getProperty("noFeatures"));
		
		sigFeatures = Integer.parseInt(props.getProperty("sigFeatures"));
		
		th = Float.parseFloat(props.getProperty("threshold"));
	}
}
