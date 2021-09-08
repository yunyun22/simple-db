package simpledb;

/**
 * @author wang, jinqiao
 * @date 25/08/2021
 */
public class Table {

    private final int tableId;

    private final String name;

    private final String primaryKeyName;

    private final DbFile dbFile;

    public Table(int tableId, String name, String primaryKeyName, DbFile dbFile) {
        this.tableId = tableId;
        this.name = name;
        this.primaryKeyName = primaryKeyName;
        this.dbFile = dbFile;
    }

    public int getTableId() {
        return tableId;
    }


    public String getName() {
        return name;
    }



    public String getPrimaryKeyName() {
        return primaryKeyName;
    }



    public DbFile getDbFile() {
        return dbFile;
    }


}
