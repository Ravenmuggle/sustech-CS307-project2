package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.physicalOperator.PhysicalOperator;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.logicalOperator.LogicalOperator;

import net.sf.jsqlparser.statement.ExplainStatement;

import org.pmw.tinylog.Logger;

public class ExplainExecutor implements DMLExecutor {

    private final ExplainStatement explainStatement;
    private final DBManager dbManager;

    public ExplainExecutor(ExplainStatement explainStatement, DBManager dbManager) {
        this.explainStatement = explainStatement;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        //todo: finish this function here, and add log info (tried)
        try {
            // 记录开始执行 EXPLAIN 语句的日志信息
            Logger.info("Starting to execute EXPLAIN statement.");

            // 提取要解释的 SQL 语句
            String sqlToExplain = explainStatement.getStatement().toString();
            Logger.info("SQL statement to explain: {}", sqlToExplain);

            // 使用 LogicalPlanner 生成逻辑计划
            Logger.debug("Generating logical plan for the SQL statement.");
            LogicalOperator logicalPlan = LogicalPlanner.resolveAndPlan(dbManager, sqlToExplain);

            // 检查逻辑计划是否生成成功
            if (logicalPlan != null) {
                Logger.info("Logical plan generated successfully: {}", logicalPlan.toString());
                // 进一步将逻辑计划转换为物理计划并记录日志
                PhysicalOperator physicalPlan = PhysicalPlanner.generateOperator(dbManager, logicalPlan);
                Logger.info("Physical plan generated successfully: {}", physicalPlan.toString());
            } else {
                Logger.error("Failed to generate logical plan for the SQL statement.");
            }

            // 记录执行结束的日志信息
            Logger.info("EXPLAIN statement execution completed.");
        } catch (DBException e) {
            // 记录异常信息
            Logger.error("An error occurred while executing EXPLAIN statement: {}", e.getMessage());
            throw e;
        }
    }
}
