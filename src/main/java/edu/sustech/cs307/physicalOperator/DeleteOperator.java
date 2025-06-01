package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;

public class DeleteOperator implements PhysicalOperator {
    private final String tableName;
    private final PhysicalOperator child;
    private final DBManager dbManager;
    private int rowsAffected;
    private boolean hasExecuted;
    private Tuple resultTuple;

    public DeleteOperator(String tableName, PhysicalOperator child, DBManager dbManager) {
        this.tableName = tableName;
        this.child = child;
        this.dbManager = dbManager;
        this.rowsAffected = 0;
        this.hasExecuted = false;
        this.resultTuple = null;
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        try {
            while (child.hasNext()) {
                child.Next();
                Tuple tuple = child.Current();

                // 从元组中获取记录ID（假设Tuple实现了getRID方法）
                RID rid = extractRIDFromTuple(tuple);
                if (rid != null) {
                    // 执行实际的删除操作
                    dbManager.getRecordManager().deleteRecord(tableName, rid);
                    rowsAffected++;
                }
            }

            // 创建结果元组，包含删除操作的状态和受影响的行数
            createResultTuple();
        } catch (Exception e) {
            throw new DBException("Error executing DELETE: " + e.getMessage(), e);
        } finally {
            hasExecuted = true;
        }
    }

    private RID extractRIDFromTuple(Tuple tuple) {
        // 实际实现需要根据元组的具体结构提取RID
        // 这里假设元组包含一个名为"__rid__"的隐藏列
        try {
            Value ridValue = tuple.getValue(new TabCol(null, "__rid__"));
            if (ridValue != null && ridValue.type == ValueType.INTEGER) {
                return new RID(ridValue.asLong());
            }
        } catch (DBException e) {
            // 处理异常
        }
        return null;
    }

    private void createResultTuple() {
        TempTuple tuple = new TempTuple();
        tuple.addValue("status", new Value("SUCCESS", ValueType.CHAR));
        tuple.addValue("rows_affected", new Value(rowsAffected, ValueType.INTEGER));
        this.resultTuple = tuple;
    }

    @Override
    public boolean hasNext() {
        return !hasExecuted;
    }

    @Override
    public void Next() throws DBException {
        if (!hasExecuted) {
            executeDelete();
        }
    }

    @Override
    public Tuple Current() {
        return resultTuple;
    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();

        ColumnMeta statusCol = new ColumnMeta();
        statusCol.name = "status";
        statusCol.type = "VARCHAR";
        statusCol.len = 20;
        schema.add(statusCol);

        ColumnMeta countCol = new ColumnMeta();
        countCol.name = "rows_affected";
        countCol.type = "INTEGER";
        countCol.len = 4;
        schema.add(countCol);

        return schema;
    }
}