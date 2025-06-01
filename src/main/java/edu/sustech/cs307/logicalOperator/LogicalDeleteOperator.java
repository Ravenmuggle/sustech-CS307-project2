package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

public class LogicalDeleteOperator extends LogicalOperator {
    private final String tableName;
    private final Expression whereCondition;
    private ArrayList<ColumnMeta> outputSchema;

    public LogicalDeleteOperator(String tableName, Expression whereCondition) {
        super();
        this.tableName = tableName;
        this.whereCondition = whereCondition;
        this.outputSchema = new ArrayList<>();
        // 通常DELETE操作不返回结果集，所以outputSchema可能为空或包含操作状态信息
        initOutputSchema();
    }

    private void initOutputSchema() {
        // DELETE操作的输出模式可以包含操作状态信息
        ColumnMeta statusColumn = new ColumnMeta();
        statusColumn.name = "status";
        statusColumn.type = ValueType.CHAR;
        statusColumn.len = 20;
        outputSchema.add(statusColumn);

        ColumnMeta countColumn = new ColumnMeta();
        countColumn.name = "rows_affected";
        countColumn.type = ValueType.INTEGER;
        countColumn.len = 4;
        outputSchema.add(countColumn);
    }

    public ArrayList<ColumnMeta> getOutputSchema() {
        return outputSchema;
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
        sb.append("LogicalDeleteOperator[");
        sb.append("table=").append(tableName);
        if (whereCondition != null) {
            sb.append(", where=").append(whereCondition.toString());
        }
        sb.append("]");
        return sb.toString();
    }
}