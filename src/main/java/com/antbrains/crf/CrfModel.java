package com.antbrains.crf;

public class CrfModel {
	public TrainingParams params;
	public TrainingWeights weights;
	public CrfModel(TrainingParams params, TrainingWeights weights){
		this.params=params;
		this.weights=weights;
	}
}
