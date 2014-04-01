package com.antbrains.crf;

import java.util.ArrayList;
import java.util.List;

public class BESB1B2MTagConvertor implements TagConvertor{
	private static final long serialVersionUID = -2727578511882941461L;
	public BESB1B2MTagConvertor(){
		tags.add("S");
		tags.add("B");
		tags.add("E");
		tags.add("B1");
		tags.add("B2");
		tags.add("M");
	}
	@Override
	public String[] tokens2Tags(String[] tokens) {
		List<String> tags=new ArrayList<String>();
		for(String word:tokens){
			if(word.length()==1){
				tags.add("S");
			}else if(word.length()==2){
				tags.add("B");
				tags.add("E");
			}else if(word.length()==3){
				tags.add("B");
				tags.add("B1");
				tags.add("E");
			}else if(word.length()==4){
				tags.add("B");
				tags.add("B1");
				tags.add("B2");
				tags.add("E");
			}else if(word.length()>4){
				tags.add("B");
				tags.add("B1");
				tags.add("B2");
				for(int i=3;i<word.length()-1;i++){
					tags.add("M");
				}
				tags.add("E");
			}
		}
		return tags.toArray(new String[0]);
	}

	@Override
	public String[] tokens2Tags(List<String> tokens) {
		List<String> tags=new ArrayList<String>();
		for(String word:tokens){
			if(word.length()==1){
				tags.add("S");
			}else if(word.length()==2){
				tags.add("B");
				tags.add("E");
			}else if(word.length()==3){
				tags.add("B");
				tags.add("B1");
				tags.add("E");
			}else if(word.length()==4){
				tags.add("B");
				tags.add("B1");
				tags.add("B2");
				tags.add("E");
			}else if(word.length()>4){
				tags.add("B");
				tags.add("B1");
				tags.add("B2");
				for(int i=3;i<word.length()-1;i++){
					tags.add("M");
				}
				tags.add("E");
			}
		}
		return tags.toArray(new String[0]);
	}
	

	@Override
	public String[] tags2Tokens(String[] tags, String sentence) {
		return tags2TokenList(tags, sentence).toArray(new String[0]);
	}

	@Override
	public List<String> tags2TokenList(String[] tags, String sentence) {
		List<String> tokens=new ArrayList<String>();
		int startPos=0;
		for(int i=0;i<tags.length;i++){
			String tag=tags[i];
			if(tag.equals("B")){
				if(i>startPos){
					System.err.println("unfinished: "+i);
					String token=sentence.substring(startPos,i-1);
					tokens.add(token);
				}
				startPos=i;
			}else if(tag.equals("S")){
				if(i>startPos){
					System.err.println("unfinished: "+i);
					String token=sentence.substring(startPos,i-1);
					tokens.add(token);
				}
				tokens.add(sentence.substring(i,i+1));
				startPos=i+1;
			}else if(tag.equals("E")){
				String token=sentence.substring(startPos,i+1);
				tokens.add(token);
				startPos=i+1;
			}
		}
		if(startPos<sentence.length()){
			System.err.println("unfinished: "+startPos);
			String token=sentence.substring(startPos,sentence.length());
			tokens.add(token);
		}
		return tokens;
	}

	@Override
	public int getTagNum() {
		return tags.size();
	}

	List<String> tags=new ArrayList<String>();
	@Override
	public List<String> getTags() {
		return tags;
	}

}
