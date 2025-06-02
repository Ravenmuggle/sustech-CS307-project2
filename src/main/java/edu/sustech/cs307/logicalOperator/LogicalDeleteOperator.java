package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.meta.ColumnMeta;
import net.sf.jsqlparser.expression.Expression;
import java.util.ArrayList;
import java.util.Collections;

public class LogicalDeleteOperator extends LogicalOperator {
    private final String tableName;
    private final Expression whereCondition;
    private ArrayList<ColumnMeta> outputSchema;

    public LogicalDeleteOperator(String tableName, Expression whereCondition) {
        super(Collections.emptyList());
        this.tableName = tableName;
        this.whereCondition = whereCondition;
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereCondition() {
        return whereCondition;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DeleteOperator(table=");
        sb.append(tableName);
        if (whereCondition != null) {
            sb.append(", where=").append(whereCondition.toString());
        }

        sb.append(")");
        return sb.toString();
    }
}