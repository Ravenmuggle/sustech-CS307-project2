package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.*;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;

public class DeleteOperator implements PhysicalOperator {
    private final String tableName;
    private final PhysicalOperator child;
    private final DBManager dbManager;
    private int numDeleted;
    private boolean outputed;


    public DeleteOperator(String tableName, PhysicalOperator child, DBManager dbManager) {
        this.tableName = tableName;
        this.child = child;
        this.dbManager = dbManager;
        this.numDeleted = 0;
        this.outputed = false;
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();

            // 从元组中获取记录ID（假设Tuple实现了getRID方法）
            if (tuple instanceof TableTuple) {
                RID rid = ((TableTuple) tuple).getRID();
                if (rid != null) {
                    // 执行实际的删除操作
                    RecordFileHandle fileHandle = dbManager.getRecordManager().OpenFile(tableName);
                    RecordPageHandle pageHandle = fileHandle.FetchPageHandle(rid.pageNum);
                    Record currentRecord = fileHandle.GetRecord(rid);
                    BitMap.reset(pageHandle.bitmap, rid.slotNum);
                }
                numDeleted++;
            } else {
                throw new DBException(ExceptionTypes.UNKNOWN_EXCEPTION);
            }
        }
    }

    @Override
    public boolean hasNext() throws DBException{
        return !this.outputed;
    }

    @Override
    public void Next(){
    }

    @Override
    public Tuple Current() {
        ArrayList<Value> values = new ArrayList<>();
        values.add(new Value(numDeleted, ValueType.INTEGER));
        this.outputed = true;
        return new TempTuple(values);
    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> outputSchema = new ArrayList<>();
        outputSchema.add(new ColumnMeta("delete", "numberOfDeleteRows", ValueType.INTEGER, 0, 0));
        return outputSchema;
    }
}