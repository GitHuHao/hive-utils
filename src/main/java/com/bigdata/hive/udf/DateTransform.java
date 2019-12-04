package com.bigdata.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DateTransform extends UDF {

	private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	public String evaluate(final String inputDate) {
		
		if (inputDate == null) {
			return null;
		}
	
		Date parseDate;
		String outputDate = null;
		try {
			// 解析
			parseDate = inputFormat.parse(inputDate);
			
			// 转换
			outputDate = outputFormat.format(parseDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return outputDate;
	}
}