package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.storage.PagePosition;
import edu.sustech.cs307.tuple.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public class DBManager {
    private final MetaManager metaManager;
    /* --- --- --- */
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final RecordManager recordManager;

    public DBManager(DiskManager diskManager, BufferPool bufferPool, RecordManager recordManager,
            MetaManager metaManager) {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
        this.recordManager = recordManager;
        this.metaManager = metaManager;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public RecordManager getRecordManager() {
        return recordManager;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public MetaManager getMetaManager() {
        return metaManager;
    }

    public boolean isDirExists(String dir) {
        File file = new File(dir);
        return file.exists() && file.isDirectory();
    }

    /**
     * Displays a formatted table listing all available tables in the database.
     * The output is presented in a bordered ASCII table format with centered table
     * names.
     * Each table name is displayed in a separate row within the ASCII borders.
     */
    public void showTables() {
        //throw new RuntimeException("Not implement");
        //todo: complete show table (finished)
        // | -- TABLE -- |
        // | -- ${table} -- |
        // | ----------- |
        Set <String> tableNames = metaManager.getTableNames();
        Logger.info(getStartEndLine(1, true));
        Logger.info(getString("Tables"));
        Logger.info(getSperator(1));
        for (String tableName : tableNames) {
            //Logger.info(getSperator(1));
            Logger.info(getString(tableName));
        }
        Logger.info(getStartEndLine(1, false));
    }

    public void descTable(String table_name) throws DBException {
        //throw new RuntimeException("Not implemented yet");
        //todo: complete describe table (finished)
        // | -- TABLE Field -- | -- Column Type --|
        // | --  ${table field} --| -- ${table type} --|
        TableMeta tableMeta = metaManager.getTable(table_name);
        ArrayList<ColumnMeta> columns = tableMeta.columns_list;
        Logger.info(getStartEndLine(2, true));
        Logger.info(getDescribeString("Field", "Type"));
        Logger.info(getSperator(2));
        for (ColumnMeta column : columns) {
            Logger.info(getDescribeString(column.name, column.type.toString()));
        }
        Logger.info(getStartEndLine(2, false));
    }

    /**
     * Creates a new table in the database with specified name and column metadata.
     * This method sets up both the table metadata and the physical storage
     * structure.
     *
     * @param table_name The name of the table to be created
     * @param columns    List of column metadata defining the table structure
     * @throws DBException If there is an error during table creation
     */
    public void createTable(String table_name, ArrayList<ColumnMeta> columns) throws DBException {
        TableMeta tableMeta = new TableMeta(
                table_name, columns);
        metaManager.createTable(tableMeta);
        String table_folder = String.format("%s/%s", diskManager.getCurrentDir(), table_name);
        File file_folder = new File(table_folder);
        if (!file_folder.exists()) {
            file_folder.mkdirs();
        }
        int record_size = 0;
        for (var col : columns) {
            record_size += col.len;
        }
        String data_file = String.format("%s/%s", table_name, "data");
        recordManager.CreateFile(data_file, record_size);
    }

    /**
     * Drops a table from the database by removing its metadata and associated
     * files.
     *
     * @param table_name The name of the table to be dropped
     * @param ifExists
     * @throws DBException If the table directory does not exist or encounters IO
     *                     errors during deletion
     */
    public void dropTable(String table_name, boolean ifExists) throws DBException {
        if (!isTableExists(table_name)) {
            if (ifExists) {
                System.out.println(1);
                return; // 表不存在，但 IF EXISTS 已指定，不报错
            }
            throw new DBException(ExceptionTypes.TABLE_DOSE_NOT_EXIST);
        }

        metaManager.dropTable(table_name);
        String data_file = String.format("%s/%s", table_name, "data");
        diskManager.DeleteFile(data_file);

        bufferPool.DeleteAllPages(data_file);
        recordManager.DeleteFile(data_file);
    }





    /**
     * Recursively deletes a directory and all its contents.
     * If the given file is a directory, it first deletes all its entries
     * recursively.
     * Finally deletes the file/directory itself.
     *
     * @param file The file or directory to be deleted
     * @throws IOException If deletion of any file or directory fails
     */
    private void deleteDirectory(File file) throws DBException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectory(entry);
                }
            }
        }
        if (!file.delete()) {
            throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + file.getAbsolutePath()));
        }
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param table the name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean isTableExists(String table) {
        return metaManager.getTableNames().contains(table);
    }

    /**
     * Closes the database manager and performs cleanup operations.
     * This method flushes all pages in the buffer pool, dumps disk manager
     * metadata,
     * and saves meta manager state to JSON format.
     *
     * @throws DBException if an error occurs during the closing process
     */
    public void closeDBManager() throws DBException {
        this.bufferPool.FlushAllPages(null);
        DiskManager.dump_disk_manager_meta(this.diskManager);
        this.metaManager.saveToJson();
    }

    private static String getString(String string) {
        StringBuilder stringBuilder = new StringBuilder("|");
        String centeredText = StringUtils.center(string, 15, ' ');
        stringBuilder.append(centeredText).append("|");
        return stringBuilder.toString();
    }

    private static String getDescribeString(String field, String type) {
        StringBuilder stringBuilder = new StringBuilder("|");
        String centeredText = StringUtils.center(field, 15, ' ');
        stringBuilder.append(centeredText).append("|");
        centeredText = StringUtils.center(type, 15, ' ');
        stringBuilder.append(centeredText).append("|");
        return stringBuilder.toString();
    }

    private static String getSperator(int width) {
        // ───────────────
        StringBuilder line = new StringBuilder("|");
        for (int i = 0; i < width; i++) {
            line.append("───────────────");
            line.append("|");
        }
        return line.toString();
    }

    private static String getStartEndLine(int width, boolean header) {
        StringBuilder end_line;
        if (header) {
            end_line = new StringBuilder("┌");
        } else {
            end_line = new StringBuilder("└");
        }
        for (int i = 0; i < width; i++) {
            end_line.append("───────────────");
            if (header) {
                end_line.append("┐");
            } else {
                end_line.append("┘");
            }
        }
        return end_line.toString();
    }
}
