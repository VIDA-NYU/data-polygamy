/* Copyright (C) 2016 New York University
   This file is part of Data Polygamy which is released under the Revised BSD License
   See file LICENSE for full license details. */
package edu.nyu.vida.data_polygamy.ct;

import java.io.Serializable;
import java.util.Arrays;

public class MyIntList implements Serializable {
    private static final long serialVersionUID = 1L;
    
    public int [] array;
	public int length = 0;
	
	public MyIntList(int no) {
		array = new int[no];
	}
	
	public MyIntList() {
		array = new int[10];
	}
	
	public void add(int n) {
		if(array == null) {
			array = new int[10];
		}
		if(length == array.length) {
			array = Arrays.copyOf(array, (int) (length * 1.5));
		}
		array[length ++] = n;
	}
	
	public int size() {
		return length;
	}
	
	public int get(int i) {
		return array[i];
	}

	public void clear() {
		array = null;
		length = 0;
	}

	public void set(int pos, int val) {
		array[pos] = val;		
	}
	
	public void remove(int val) {
		int in = -1;
		for(int i = 0;i < length;i ++) {
			if(array[i] == val) {
				in = i;
				break;
			}
		}
		if(in != -1) {
			array[in] = array[length - 1];
			length --;
		}
	}
	
	public void removeElement(int el) {
		int i = 0;
		for(;i < length;i ++) {
			if(array[i] == el) {
				break;
			}
		}
		if(i == length) {
			return;
		}
		for(;i < length - 1;i ++) {
			array[i] = array[i + 1];
		}
		length --;
	}
	
	public void replace(int replacee, int replacer) {
		for(int i = 0;i < length;i ++) {
			if(array[i] == replacee) {
				array[i] = replacer;
				return;
			}
		}
		System.out.println("Could not find element!!");		
	}
	
	public boolean contains(int el) {
		for(int i = 0;i < length;i ++) {
			if(array[i] == el) {
				return true;
			}
		}
		return false;
	}
	
	public void addAll(MyIntList list) {
		for(int i = 0;i < list.length;i ++) {
			add(list.get(i));
		}
	}
	
	public void resize() {
		array = Arrays.copyOf(array, (int) (length));
	}
}
