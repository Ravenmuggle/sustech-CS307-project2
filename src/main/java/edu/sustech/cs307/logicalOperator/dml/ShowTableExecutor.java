package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import org.pmw.tinylog.Logger;

public class ShowTableExecutor implements DMLExecutor{
    DBManager dbManager;
    ShowTablesStatement showTablesStatement;

    public ShowTableExecutor(ShowTablesStatement showTablesStatement, DBManager dbManager){
        this.showTablesStatement=showTablesStatement;
        this.dbManager=dbManager;
    }
    public void execute() throws DBException {
            try{
                showTables();
            }
            catch(Exception e) {
               throw new DBException(ExceptionTypes.UnsupportedCommand(String.format("SHOW")));
            }
    }
    private void showTables() throws DBException {
        Logger.info("|-----------|");
        Logger.info("|  Tables   |");
        Logger.info("|-----------|");
        for (String table:dbManager.getMetaManager().getTableNames()){
            Logger.info("|     "+table+"     |");
        }
        Logger.info("|-----------|");
    }
}
