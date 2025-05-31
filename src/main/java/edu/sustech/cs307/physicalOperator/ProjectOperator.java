package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.ProjectTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private List<TabCol> outputSchema; // Use bounded wildcard
    private Tuple currentTuple;

    public ProjectOperator(PhysicalOperator child, List<TabCol> outputSchema) throws DBException { // Use bounded wildcard
        this.child = child;
        this.outputSchema = outputSchema;
        if (this.outputSchema.size() == 1 && this.outputSchema.get(0).getTableName() != null && this.outputSchema.get(0).getTableName().equals("*")) {
            List<TabCol> newOutputSchema = new ArrayList<>();
            for (ColumnMeta tabCol : child.outputSchema()) {
                newOutputSchema.add(new TabCol(tabCol.tableName, tabCol.name));
            }
            this.outputSchema = newOutputSchema;
        } else {
            List<TabCol> newOutputSchema = new ArrayList<>();
            for (TabCol tabCol : outputSchema) {
                if (tabCol.getTableName() == null) {
                    ArrayList<ColumnMeta> childSchema = child.outputSchema();
                    boolean flg =  true;
                    for (ColumnMeta columnMeta : childSchema) {
                        if (columnMeta.name.equals(tabCol.getColumnName())) {
                            newOutputSchema.add(new TabCol(columnMeta.tableName, tabCol.getColumnName()));
                            flg = false;
                            break;
                        }
                    }
                    // 如果仍然无法推断表名，抛出异常
                    if (flg) {
                        throw new DBException(ExceptionTypes.ColumnDoseNotExist(tabCol.getColumnName()));
                    }
                } else {
                    newOutputSchema.add(tabCol);
                }
            }
            this.outputSchema = newOutputSchema;
        }

    }

    @Override
    public boolean hasNext() throws DBException {
        return child.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
    }

    @Override
    public void Next() throws DBException {
        if (hasNext()) {
            child.Next();
            Tuple inputTuple = child.Current();
            if (inputTuple != null) {

                currentTuple = new ProjectTuple(inputTuple, outputSchema); // Create ProjectTuple
            } else {
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
        child.Close();
        currentTuple = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        //todo: return the fields only appear in select item (finished)
        //return child.outputSchema();

        ArrayList<ColumnMeta> result = new ArrayList<>();
        for (TabCol tabCol : outputSchema) {
            for (ColumnMeta columnMeta : child.outputSchema()) {
                if (tabCol.getTableName().equals(columnMeta.tableName) &&
                        tabCol.getColumnName().equals(columnMeta.name)) {
                    result.add(columnMeta);
                    break;
                }
            }
        }
        return result;
    }
}
