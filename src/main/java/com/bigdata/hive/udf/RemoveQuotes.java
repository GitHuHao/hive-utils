package com.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class RemoveQuotes extends UDF {

	public String evaluate(final String s) {
		
		if (s == null) {
			return null;
		}
		
		return s.toString().replace("\"", "");
	}
}