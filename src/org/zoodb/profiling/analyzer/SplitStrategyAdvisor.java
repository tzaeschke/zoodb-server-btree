package org.zoodb.profiling.analyzer;

public class SplitStrategyAdvisor {
	
	private ISplitStrategy strategy;

	
	public SplitStrategyAdvisor(ISplitStrategy s) {
		this.strategy = s;
	}
	
	public int checkForSplit(FieldCount[] fc, Class<?> c) {
		return strategy.getSplitIndex(fc,c);
	}
	

}
