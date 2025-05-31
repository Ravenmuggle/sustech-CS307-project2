package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import org.pmw.tinylog.Logger;
import edu.sustech.cs307.exception.ExceptionTypes;

public class DropTableExector implements DMLExecutor {
    private final Drop dropTableStmt;
    private final DBManager dbManager;
    private final String sql;
    public DropTableExector(Drop dropTable,DBManager dbManager,String sql){
        this.dropTableStmt = dropTable;
        this.dbManager = dbManager;
        this.sql = sql;
    }
    public void execute() throws DBException {
        String tableName= dropTableStmt.getName().getName();
        boolean ifExists = dropTableStmt.isIfExists();   //检查是否有“if exist”语句
        try{
            dbManager.dropTable(tableName,ifExists);
            Logger.info("Dropped table "+tableName+" successfully");
        }catch (DBException e){
            if (ifExists && e.getType()== ExceptionTypes.TABLE_DOSE_NOT_EXIST){
                Logger.info("Table {} does not exist (IF EXISTS was specified)", tableName);
                return; // 如果 IF EXISTS 且表不存在，不报错
            }
            throw e;
        }
    }


}
