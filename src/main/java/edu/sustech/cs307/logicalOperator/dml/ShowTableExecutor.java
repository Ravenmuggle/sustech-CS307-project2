package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;

public class ShowTableExecutor implements DMLExecutor{
    DBManager dbManager;
    ShowTablesStatement showTablesStatement;

    public ShowTableExecutor(ShowTablesStatement showTablesStatement, DBManager dbManager){
        this.showTablesStatement = showTablesStatement;
        this.dbManager = dbManager;
    }
    public void execute() {
        dbManager.showTables();
    }
}
