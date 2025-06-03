package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

public class IndexScanOperator implements PhysicalOperator {
    private InMemoryOrderedIndex index;
    private Iterator<Entry<edu.sustech.cs307.value.Value, RID>> iterator;
    private Tuple currentTuple;
    private DBManager dbManager;
    private String tableName;

    public IndexScanOperator(InMemoryOrderedIndex index, DBManager dbManager, String tableName) {
        this.index = index;
        this.dbManager = dbManager;
        this.tableName = tableName;
    }

    @Override
    public boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        // 假设这里从索引中获取所有记录的迭代器
        iterator = index.Range(null, null, false, false); // 获取全表扫描的迭代器，可根据实际需求修改
    }

    @Override
    public void Next() {
        if (hasNext()) {
            Entry<edu.sustech.cs307.value.Value, RID> entry = iterator.next();
            RID rid = entry.getValue();
            try {
                Record record = dbManager.getRecordManager().OpenFile(tableName).GetRecord(rid);
                currentTuple = new TableTuple(tableName, dbManager.getMetaManager().getTable(tableName), record, rid);
            } catch (DBException e) {
                e.printStackTrace();
                currentTuple = null;
            }
        } else {
            currentTuple = null;
        }
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        iterator = null;
        currentTuple = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        try {
            return dbManager.getMetaManager().getTable(tableName).columns_list;
        } catch (DBException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
}