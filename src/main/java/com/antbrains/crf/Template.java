package com.antbrains.crf;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Template implements Serializable {
	private static final long serialVersionUID = 8936075816725599450L;
	public static final String[] BOS = {
		"_B-1", "_B-2", "_B-3", "_B-4"
	};
	public static final String[] EOS = {
		"_B+1", "_B+2", "_B+3", "_B+4"
	};
	private String[] patterns;
	
	public Template(String[] patterns) {
		this.patterns = patterns;
	}
	
	public Template(String templateFilename, String charset) throws IOException {
		BufferedReader br = null;
		List<String> patternList = new ArrayList<String>();
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(templateFilename), charset));
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.length() == 0 || line.startsWith("#")) {
					continue;
				}
				patternList.add(line);
			}
		} finally {
			if (br != null) {
				br.close();
			}
		}
		this.patterns = patternList.toArray(new String[patternList.size()]);
	}
	
	public String[] getPatterns() {
		return patterns;
	}

	public void setPatterns(String[] patterns) {
		this.patterns = patterns;
	}

	private String expandPattern(String pattern, int itemIndex, List<String> attributes, int rowSize) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < pattern.length(); i++) {
			char c = pattern.charAt(i);
			switch (c) {
			case '%':
				i++;
				if (i >= pattern.length()) {
					throw new IllegalStateException("template error: " + pattern);
				}
				char nextChar = pattern.charAt(i);
				switch (nextChar) {
				case 'x':
					i++;
					i = getIndex(sb, pattern, i, itemIndex, attributes, rowSize);
					break;
				default:
					throw new IllegalStateException("template error: " + pattern);
				}
				break;
			default:
				sb.append(c);
			}
		}
		return sb.toString();
	}
	
	private int getIndex(StringBuilder sb, String template, int index, int pos, List<String> attributes, int rowSize) {
		char firstChar = template.charAt(index++);
		if (firstChar != '[') {
			throw new IllegalStateException("template error: " + template);
		}
		int col = 0, row = 0;
		int neg = 1;
		if (template.charAt(index++) == '-') {
			neg = -1;
		} else {
			index--;
		}
		
		NEXT_ROW:
		for (; index < template.length(); index++) {
			char c = template.charAt(index);
			switch (c) {
			case '0': 
			case '1': 
			case '2': 
			case '3': 
			case '4': 
			case '5': 
			case '6': 
			case '7': 
			case '8': 
			case '9':
				row = row * 10 + (c - '0');
				break;
			case ',':
				index++;
				break NEXT_ROW;
			default: 
				throw new IllegalStateException("wrong template format: " + template);
			}
		}
		
		NEXT_COLUMN:
		for (; index < template.length(); index++) {
			char c = template.charAt(index);
			switch (c) {
			case '0': 
			case '1': 
			case '2': 
			case '3': 
			case '4': 
			case '5': 
			case '6': 
			case '7': 
			case '8': 
			case '9':
				col = col * 10 + (c - '0');
				break;
			case ']':
				break NEXT_COLUMN;
			default:
				throw new IllegalStateException("template error: " + template);
			}
		}
		
		row *= neg;
		
		int itemNum = attributes.size() / rowSize;
		int idx = pos + row;
		if (idx < 0) {
			sb.append(BOS[-idx - 1]);
		} else if (idx >= itemNum) {
			sb.append(EOS[idx - itemNum]);
		} else {
			sb.append(attributes.get(idx * rowSize + col));
		}
		return index;
	}
	
	public List<String> expandTemplate(List<String> attributes, int itemNum) {
		if (attributes == null) {
			return Collections.emptyList();
		}
		int rowSize = attributes.size() / itemNum;
		List<String> expandedAttributes = new ArrayList<String>(itemNum * length());
		for (int itemIndex = 0; itemIndex < itemNum; itemIndex++) {
			for (int patternIndex = 0; patternIndex < length(); patternIndex++) {
				expandedAttributes.add(expandPattern(patterns[patternIndex], itemIndex, attributes, rowSize));
			}
		}
		return expandedAttributes;
	}
	
	public int length() {
		return patterns.length;
	}
	public int size() {
		return patterns.length;
	}
}
