package com.bigdata.acadgild;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ConvertDateIntoFormat extends EvalFunc<String>{

	@Override
	public String exec(Tuple arg0) throws IOException {
		if ( arg0 == null || arg0.size() == 0 )
			return null;
		String date = (String) arg0.get(0);
		if ( date == null || date.isEmpty() )
			return null;
		SimpleDateFormat sdf1 = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aaa");
		try {
			Date d = sdf1.parse(date);
			SimpleDateFormat sdf2 = new SimpleDateFormat("MM/dd/yyyy");			
			String newdate =  sdf2.format(d);
			return newdate;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
