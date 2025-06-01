package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.DescribeStatement;

public class DescribeExecutor implements DMLExecutor{
    DBManager dbManager;
    DescribeStatement describeStatement;

    public DescribeExecutor(DescribeStatement describeStatement, DBManager dbManager){
        this.describeStatement = describeStatement;
        this.dbManager = dbManager;
    }
    public void execute() throws DBException {
        String table = describeStatement.getTable().getName();
        dbManager.descTable(table);
    }
}
