package com.antbrains.crf;

public class PrintTrainingProgress implements TrainingProgress{

	@Override
	public void startTraining() {
		System.out.println(new java.util.Date()+" start training...");
	}

	@Override
	public void doIter(int iter) {
		System.out.println(new java.util.Date()+" iter "+iter);	
	}

	@Override
	public void finishTraining() {
		System.out.println(new java.util.Date()+" finish training.");		
	}

	@Override
	public void doValidate(String s) {
		System.out.println("validate result");
		System.out.println(s);
	}

}
