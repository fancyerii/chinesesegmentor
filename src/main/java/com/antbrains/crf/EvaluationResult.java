package com.antbrains.crf;
 	
public class EvaluationResult {
	public static final int TP_INDEX = 0;
	public static final int FP_INDEX = 1;
	public static final int TN_INDEX = 2;
	public static final int FN_INDEX = 3;
	public int totalItemCount;
	public int correctItemCount;
	public int totalSeqCount;
	public int correctSeqCount;
	public int[][] labelIndex2count;
	private String[] labels;
	public EvaluationResult(String[] labels) {
		this.labels = labels;
		labelIndex2count = new int[labels.length][];
		for (int labelIndex = 0; labelIndex < labels.length; labelIndex++) {
			labelIndex2count[labelIndex] = new int[4];
		}
	}
	public void merge(EvaluationResult that) {
		this.totalItemCount += that.totalItemCount;
		this.correctItemCount += that.correctItemCount;
		this.totalSeqCount += that.totalSeqCount;
		this.correctSeqCount += that.correctSeqCount;
		for (int i = 0; i < labelIndex2count.length; i++) {
			int[] count = labelIndex2count[i];
			for (int j = 0; j < count.length; j++) {
				count[j] += that.labelIndex2count[i][j];
			}
		}
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < labels.length; i++) {
			String label = labels[i];
			int[] counts = labelIndex2count[i];
			int TP = counts[TP_INDEX], FP = counts[FP_INDEX], FN = counts[FN_INDEX];
			double precision = TP * 1.0 / (TP + FP);
			double recall = TP * 1.0 / (TP + FN);
			String labelReport = String.format("%s: precision=%.4f%% (%d/%d); recall=%.4f%% (%d/%d)", label, precision * 100, TP, TP + FP, recall * 100, TP, TP + FN);
			sb.append(labelReport).append("\n");
		}
		double itemAccuracy = correctItemCount * 1.0 / totalItemCount;
		double seqAccuracy = correctSeqCount * 1.0 / totalSeqCount;
		String grossReport = String.format("item accuracy = %.4f%% (%d/%d); sentence accuracy = %.4f%% (%d/%d)", itemAccuracy * 100, correctItemCount, totalItemCount, seqAccuracy * 100, correctSeqCount, totalSeqCount);
		sb.append(grossReport);
		return sb.toString();
	}
	public double getAccuracy() {
		return correctItemCount * 1.0 / totalItemCount;
	}
}