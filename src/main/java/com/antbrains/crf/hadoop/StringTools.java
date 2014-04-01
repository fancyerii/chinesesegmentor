package com.antbrains.crf.hadoop;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

public class StringTools {
	public static String normalizeQuery(String s){
		return s.toLowerCase();
	}
	
	public static String transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return value;
	}
}
