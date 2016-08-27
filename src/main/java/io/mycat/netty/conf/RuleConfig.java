package io.mycat.netty.conf;

import io.mycat.netty.router.partition.AbstractPartition;

/**
 * Created by snow_young on 16/8/27.
 */
public class RuleConfig {
    private final String column;
    private final String functionName;
    private AbstractPartition ruleAlgorithm;

    public RuleConfig(String column, String functionName) {
        if (functionName == null) {
            throw new IllegalArgumentException("functionName is null");
        }
        this.functionName = functionName;
        if (column == null || column.length() <= 0) {
            throw new IllegalArgumentException("no rule column is found");
        }
        this.column = column;
    }

    public AbstractPartition getRuleAlgorithm() {
        return ruleAlgorithm;
    }

    public void setRuleAlgorithm(AbstractPartition ruleAlgorithm) {
        this.ruleAlgorithm = ruleAlgorithm;
    }

    /**
     * @return unmodifiable, upper-case
     */
    public String getColumn() {
        return column;
    }

    public String getFunctionName() {
        return functionName;
    }
}
