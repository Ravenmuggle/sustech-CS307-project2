package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.ShowStatement;
import org.pmw.tinylog.Logger;

public class ShowDatabaseExecutor implements DMLExecutor {
    ShowStatement showStatement;
    public ShowDatabaseExecutor(ShowStatement showStatement) {
        this.showStatement = showStatement;
    }
    @Override
    public void execute() throws DBException {
        String command = showStatement.getName();
        if (command.equalsIgnoreCase("DATABASES")) {
            showDatabases();// we only have one database
        } else {
            throw new DBException(ExceptionTypes.UnsupportedCommand(String.format("SHOW %s", command)));
        }
    }
    private void showDatabases() throws DBException {
        Logger.info("|-----------|");
        Logger.info("| Databases |");
        Logger.info("|-----------|");
        Logger.info("|   CS307   |");
        Logger.info("|-----------|");
    }


}
