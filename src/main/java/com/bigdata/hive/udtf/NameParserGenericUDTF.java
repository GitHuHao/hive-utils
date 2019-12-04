
package com.bigdata.hive.udtf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * cat ./people.txt
 *
 * John Smith
 * John and Ann White
 * Ted Green
 * Dorothy
 *
 * CREATE EXTERNAL TABLE people (name string)
 * ROW FORMAT DELIMITED FIELDS
 * 	TERMINATED BY '\t'
 * 	ESCAPED BY ''
 * 	LINES TERMINATED BY '\n';
 *
 *
	add jar /opt/softwares/hive-1.2.1/lib/hive-utils-1.0-SNAPSHOT.jar;
 *
	CREATE TEMPORARY FUNCTION process_names as 'com.bigdata.hive.udtf.NameParserGenericUDTF';
 *
	SELECT adTable.name,adTable.surname FROM people lateral view process_names(name) adTable as name, surname;
 *
 */

public class NameParserGenericUDTF extends GenericUDTF {
	  private PrimitiveObjectInspector stringOI = null;

	  @Override
	  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
	  	// 只有一个入参
	    if (args.length != 1) {
	      throw new UDFArgumentException("NameParserGenericUDTF() takes exactly one argument");
	    }

	    // 入参类型必须为 字符串
	    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
	        && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
	      throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter");
	    }

	    // input 输入流
	    stringOI = (PrimitiveObjectInspector) args[0];

	    // output 
	    List<String> fieldNames = new ArrayList<String>(2);
	    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
	    fieldNames.add("name");
	    fieldNames.add("surname");
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    // 绑定 每行 字段名称，字段类型
	    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	  }
	  	  
	  public ArrayList<Object[]> processInputRecord(String name){
		    ArrayList<Object[]> result = new ArrayList<Object[]>();
		  
		    // ignoring null or empty input
		    if (name == null || name.isEmpty()) {
		      return result;
		    }
		    
		    String[] tokens = name.split("\\s+");
		    
		    if (tokens.length == 2){
		    	result.add(new Object[] { tokens[0], tokens[1] });
		    }else if (tokens.length == 4 && tokens[1].equals("and")){
		    	result.add(new Object[] { tokens[0], tokens[3] });
		    	result.add(new Object[] { tokens[2], tokens[3] });
		    }
		    
		    return result;
	  }
	  
	  @Override
	  public void process(Object[] record) throws HiveException {
	  	// 遍历
	    final String name = stringOI.getPrimitiveJavaObject(record[0]).toString();
	    ArrayList<Object[]> results = processInputRecord(name);
	    
	    Iterator<Object[]> it = results.iterator();
	    
	    while (it.hasNext()){
	    	Object[] r = it.next();
	    	forward(r);
	    }
	  }

	  @Override
	  public void close() throws HiveException {
	    // do nothing
	  }
}