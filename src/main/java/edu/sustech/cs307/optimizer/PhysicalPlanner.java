package edu.sustech.cs307.optimizer;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.*;
import edu.sustech.cs307.physicalOperator.*;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.select.Values;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.ObjectUtils.Null;

public class PhysicalPlanner {
    public static PhysicalOperator generateOperator(DBManager dbManager, LogicalOperator logicalOp) throws DBException {
        if (logicalOp instanceof LogicalTableScanOperator tableScanOperator) {
            return handleTableScan(dbManager, tableScanOperator);
        } else if (logicalOp instanceof LogicalFilterOperator filterOperator) {
            return handleFilter(dbManager, filterOperator);
        } else if (logicalOp instanceof LogicalJoinOperator joinOperator) {
            return handleJoin(dbManager, joinOperator);
        } else if (logicalOp instanceof LogicalProjectOperator projectOperator) {
            return handleProject(dbManager, projectOperator);
        } else if (logicalOp instanceof LogicalInsertOperator insertOperator) {
            return handleInsert(dbManager, insertOperator);
        } else if (logicalOp instanceof LogicalUpdateOperator updateOperator) {
            return handleUpdate(dbManager, updateOperator);
        } else if (logicalOp instanceof LogicalDeleteOperator deleteOperator) {
            return handleDelete(dbManager, deleteOperator);
        } else {
            throw new DBException(ExceptionTypes.UnsupportedOperator(logicalOp.getClass().getSimpleName()));
        }
    }

    private static PhysicalOperator handleTableScan(DBManager dbManager, LogicalTableScanOperator logicalTableScanOp) {
        String tableName = logicalTableScanOp.getTableName();
        TableMeta tableMeta;
        try {
            tableMeta = dbManager.getMetaManager().getTable(tableName);
        } catch (DBException e) {
            // Fallback to SeqScan if TableMeta cannot be retrieved
            return new SeqScanOperator(tableName, dbManager);
        }

        // Check if index exists for the table (for now, assume RBTreeIndex always
        // exists if index is defined)
        if (tableMeta.getIndexes() != null && !tableMeta.getIndexes().isEmpty()) {
            throw new RuntimeException("unimplement");
        } else {
            return new SeqScanOperator(tableName, dbManager);
        }
    }

    private static PhysicalOperator handleFilter(DBManager dbManager, LogicalFilterOperator logicalFilterOp)
            throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalFilterOp.getChild());
        return new FilterOperator(inputOp, logicalFilterOp.getWhereExpr());
    }

    private static PhysicalOperator handleJoin(DBManager dbManager, LogicalJoinOperator logicalJoinOp)
            throws DBException {
        PhysicalOperator leftOp = generateOperator(dbManager, logicalJoinOp.getLeftInput());
        PhysicalOperator rightOp = generateOperator(dbManager, logicalJoinOp.getRightInput());
        PhysicalOperator joinOp = new NestedLoopJoinOperator(leftOp, rightOp, logicalJoinOp.getJoinExprs());

        Collection<Expression> joinFilters = logicalJoinOp.getJoinExprs();
        PhysicalOperator finalOp = new FilterOperator(joinOp, joinFilters);

        return finalOp;
    }

    private static PhysicalOperator handleProject(DBManager dbManager, LogicalProjectOperator logicalProjectOp)
            throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalProjectOp.getChild());
        return new ProjectOperator(inputOp, logicalProjectOp.getOutputSchema());
    }

    /**
     * 处理将逻辑插入操作转换为物理插入运算符的过程
     * 
     * @param dbManager       提供数据库操作访问的数据库管理器实例
     * @param logicalInsertOp 需要被转换的逻辑插入运算符
     * @return 准备好执行的物理插入运算符
     * @throws DBException 如果存在列不匹配、类型不匹配或无效SQL语法时抛出
     */
    @SuppressWarnings("deprecation") // for ExpressionList<?>::getExpressions
//     private static PhysicalOperator handleInsert(DBManager dbManager, LogicalInsertOperator logicalInsertOp)
//         throws DBException {
//     var tableMeta = dbManager.getMetaManager().getTable(logicalInsertOp.tableName);

//     // Process columns
//     List<String> columns = new ArrayList<>();
//     List<Value> values = new ArrayList<>();

//     if (logicalInsertOp.columns != null && !logicalInsertOp.columns.isEmpty()) {
//         if (tableMeta.columns.size() < logicalInsertOp.columns.size()) {
//             throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
//         }

//         // Map specified columns to table columns
//         for (ColumnMeta columnMeta : tableMeta.columns_list) {
//             boolean columnSpecified = false;
//             for (int i = 0; i < logicalInsertOp.columns.size(); i++) {
//                 String colName = logicalInsertOp.columns.get(i).getColumnName();
//                 if (columnMeta.name.equals(colName)) {
//                     columns.add(colName);
//                     columnSpecified = true;
//                     break;
//                 }
//             }
//             // 如果列未指定，填充为 NULL
//             if (!columnSpecified) {
//                 columns.add(columnMeta.name);
//                 values.add(null); // 插入 NULL 值
//             }
//         }
//     } else {
//         // 如果未指定列，使用所有表列并填充默认值或 NULL
//         for (ColumnMeta columnMeta : tableMeta.columns_list) {
//             columns.add(columnMeta.name);
//             values.add(null); // 插入 NULL 值
//         }
//     }

//     // Validate and parse VALUES clause
//     if (!(logicalInsertOp.values instanceof Values)) {
//         throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Values must be an expression list"));
//     }
//     ExpressionList<?> valuesList = ((Values) logicalInsertOp.values).getExpressions();
//     if (valuesList.size() > columns.size()) {
//         throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
//     }

//     // Parse values and fill missing columns with NULL
//     parseValue(values, valuesList, tableMeta, columns);

//     return new InsertOperator(logicalInsertOp.tableName, columns, values, dbManager);
// }
    private static PhysicalOperator handleInsert(DBManager dbManager, LogicalInsertOperator logicalInsertOp)
            throws DBException {
        var tableMeta = dbManager.getMetaManager().getTable(logicalInsertOp.tableName);
        // Process columns
        List<String> columns = new ArrayList<>();
        List<Value> values = new ArrayList<>();
        ExpressionList<?> valuesList = ((Values) logicalInsertOp.values).getExpressions();
        if (logicalInsertOp.columns != null) {
            if (tableMeta.columns.size() < logicalInsertOp.columns.size()) {
                System.out.println(tableMeta.columns.size());
                throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
            }
            else if (logicalInsertOp.columns.size() != 0) {
                boolean insertValid = true;
                String exitColunm="";  //which column does not exist
                for (int i = 0; i < logicalInsertOp.columns.size(); i++) {
                String colName = logicalInsertOp.columns.get(i).getColumnName();
                for (ColumnMeta columnMeta : tableMeta.columns_list) {
                    if (columnMeta.name.equals(colName)) {
                        insertValid = true;
                        break;
                    } else {
                        exitColunm = colName;
                        insertValid = false;
                    }
                }
                }
                if (insertValid == false) {
                    throw new DBException(ExceptionTypes.ColumnDoseNotExist(exitColunm));
                }
                else{
                    System.out.println(tableMeta.columns_list);
                    for (int k = 0; k < tableMeta.columns_list.size(); k++) {
                        ColumnMeta columnMeta=tableMeta.columns_list.get(k);
                        boolean tableExist=false;
                        for(int j=0;j<logicalInsertOp.columns.size();j++){
                            String colName = logicalInsertOp.columns.get(j).getColumnName();
                            // Check if the column exists in the table
                            System.out.println(colName);
                            if (columnMeta.name.equals(colName)) {
                                columns.add(colName);
                                System.out.print(colName);
                                parseValue(values, valuesList, tableMeta,j,k);
                                tableExist=true;
                                break;
                            }   
                        }
                        System.out.println(tableExist);
                        if (!tableExist) {
                            columns.add(columnMeta.name);
                            values.add(new Value());
                        }
                    }
                }
            }
            }
         else {
            // If no columns specified, use all table columns in order
            for (ColumnMeta columnMeta : tableMeta.columns_list) {
                columns.add(columnMeta.name);
            }
        if (!(logicalInsertOp.values instanceof Values)) {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Values must be an expression list"));
        }

    }

        // if (columns.size() != valuesList.size()) {
        //     var element = valuesList.get(0);
        //     if (element instanceof ParenthesedExpressionList<?> parenthesed) {
        //         // check the children reexpressions
        //         for (Expression expr : valuesList) {
        //             if (expr instanceof ParenthesedExpressionList<?> expressionList) {
        //                 if (expressionList.getExpressions().size() != columns.size()) {
        //                     throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
        //                 }
        //             } else {
        //                 throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
        //             }
        //         }
        //     } else {
        //         throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
        //     }
        // }
        return new InsertOperator(logicalInsertOp.tableName, columns,
                values, dbManager);
    }

    @SuppressWarnings("deprecation")
    private static void parseValue(List<Value> values, ExpressionList<?> valuesList, TableMeta tableMeta, int i,int j)
            throws DBException {
        var expr = valuesList.get(i);
        System.out.println(expr);
        if (expr instanceof StringValue string_value) {
            if (tableMeta.columns_list.get(j).type != ValueType.CHAR
                    && tableMeta.columns_list.get(j).type != ValueType.VARCHAR) {
                throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
            }
            String value_str = string_value.getValue();
            if (value_str.length() > 64) {
                value_str = value_str.substring(0, 64);
            }
            values.add(new Value(value_str));
        } else if (expr instanceof DoubleValue float_value) {
            if (tableMeta.columns_list.get(j).type != ValueType.FLOAT
                    && tableMeta.columns_list.get(j).type != ValueType.DOUBLE) {
                throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
            }
            values.add(new Value(float_value.getValue()));
        } else if (expr instanceof LongValue long_value) {
            if (tableMeta.columns_list.get(j).type != ValueType.INTEGER) {
                throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
            }
            values.add(new Value(long_value.getValue()));
        } else if (expr instanceof Null null_value) {
            if (tableMeta.columns_list.get(j).type != ValueType.UNKNOWN) {
                throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
            }
        } else {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported value type in VALUES clause"));
        }
    }
    // private static void parseValue(List<Value> values, ExpressionList<?>
    // valuesList, TableMeta tableMeta, List<String> columns)
    // throws DBException {
    // for (int i = 0; i < columns.size(); i++) {
    // if (i < valuesList.size()) {
    // var expr = valuesList.getExpressions().get(i);
    // if (expr instanceof StringValue string_value) {
    // if (tableMeta.columns_list.get(i).type != ValueType.CHAR) {
    // throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
    // }
    // String value_str = string_value.getValue();
    // if (value_str.length() > 64) {
    // value_str = value_str.substring(0, 64);
    // }
    // values.add(new Value(value_str));
    // } else if (expr instanceof DoubleValue float_value) {
    // if (tableMeta.columns_list.get(i).type != ValueType.FLOAT) {
    // throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
    // }
    // values.add(new Value(float_value.getValue()));
    // } else if (expr instanceof LongValue long_value) {
    // if (tableMeta.columns_list.get(i).type != ValueType.INTEGER) {
    // throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
    // }
    // values.add(new Value(long_value.getValue()));
    // } else if (expr instanceof ParenthesedExpressionList<?> expressionList) {
    // parseValue(values, expressionList, tableMeta, columns);
    // } else {
    // throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported value
    // type in VALUES clause"));
    // }
    // } else {
    // // 如果值未指定，填充为 NULL
    // values.add(null);
    // }
    // }
    // }

    private static PhysicalOperator handleUpdate(DBManager dbManager, LogicalUpdateOperator logicalUpdateOp)
            throws DBException {
        // TODO: Implement handleUpdate
        PhysicalOperator scanner = generateOperator(dbManager, logicalUpdateOp.getChild());
        if (logicalUpdateOp.getColumns().size() != 1) {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported expression list"));
        }
        return new UpdateOperator(scanner, logicalUpdateOp.getTableName(), logicalUpdateOp.getColumns().get(0),
                logicalUpdateOp.getExpression());
    }

    private static PhysicalOperator handleDelete(DBManager dbManager, LogicalDeleteOperator logicalDeleteOp)
            throws DBException {
        // 获取表元数据
        TableMeta tableMeta = dbManager.getMetaManager().getTable(logicalDeleteOp.getTableName());

        // 创建SeqScan操作符用于扫描表中所有记录
        // TODO: Implement IndexScan
        SeqScanOperator scanOperator = new SeqScanOperator(logicalDeleteOp.getTableName(), dbManager);

        PhysicalOperator operator = scanOperator;

        // 如果有WHERE条件，添加Filter操作符进行过滤
        if (logicalDeleteOp.getWhereCondition() != null) {
            operator = new FilterOperator(operator,logicalDeleteOp.getWhereCondition());
        }

        // 创建Delete操作符执行实际删除
        return new DeleteOperator(logicalDeleteOp.getTableName(), operator, dbManager);
    }
}
