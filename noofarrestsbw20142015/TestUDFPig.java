package com.bigdata.acadgild;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class TestUDFPig {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ConvertDateIntoFormat c = new ConvertDateIntoFormat();
		Tuple t = new Tuple() {
			private List<Object> elements = new ArrayList<>();
			@Override
			public Iterator<Object> iterator() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public int compareTo(Object o) {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public void write(DataOutput arg0) throws IOException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void readFields(DataInput arg0) throws IOException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public String toDelimitedString(String arg0) throws ExecException {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public int size() {
				// TODO Auto-generated method stub
				return 1;
			}
			
			@Override
			public void set(int arg0, Object arg1) throws ExecException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void reference(Tuple arg0) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public boolean isNull(int arg0) throws ExecException {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public byte getType(int arg0) throws ExecException {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public long getMemorySize() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public List<Object> getAll() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Object get(int arg0) throws ExecException {

				return "09/01/2014 11:23:22 AM";
			}
			
			@Override
			public void append(Object arg0) {
				// TODO Auto-generated method stub
				
			}
		};
		
		try {
			System.out.println(c.exec(t));
		} catch (IOException e) {
			e.printStackTrace();
		}
		 

	}

}
