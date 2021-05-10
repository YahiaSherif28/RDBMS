import java.io.*;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static final String NEXT_PAGE_NAME_PATH = "src/main/resources/data/nextPageName.class";
    private static final String TABLES_FILE_PATH = "src/main/resources/data/tables.class";
    private static final String PAGES_FILE_PATH = "src/main/resources/data/pages/";
    private static final String RESOURCES_PATH = "src/main/resources/";
    private static final String CONFIG_FILE = "DBApp.config";

    private static Integer MaximumRowsCountinTablePage;
    private static Integer MaximumKeysCountinIndexBucket;

    private Vector<Table> tables;

    public DBApp() {
        init();
    }

    public static int getMaximumRowsCountinTablePage() {
        return MaximumRowsCountinTablePage;
    }

    public static int getMaximumKeysCountinIndexBucket() {
        return MaximumKeysCountinIndexBucket;
    }

    private static int getNextPageNameAsInt() throws IOException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(NEXT_PAGE_NAME_PATH));
        Integer nextPageName = oi.readInt();
        nextPageName++;
        oi.close();
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(NEXT_PAGE_NAME_PATH));
        os.writeInt(nextPageName);
        os.close();
        return nextPageName;
    }

    public static String getNextPageName() throws IOException {
        return PAGES_FILE_PATH + String.valueOf(getNextPageNameAsInt()) + ".class";
    }

    public void reserialize() {
        ObjectOutputStream os = null;
        try {
            os = new ObjectOutputStream(new FileOutputStream(TABLES_FILE_PATH));
            os.writeObject(tables);
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void init() {

        try {
            Properties p = new Properties();
            p.load(new FileReader(RESOURCES_PATH + CONFIG_FILE));
            MaximumRowsCountinTablePage = Integer.parseInt(p.getProperty(new String("MaximumRowsCountinPage")));
            MaximumKeysCountinIndexBucket = Integer.parseInt(p.getProperty(new String("MaximumKeysCountinIndexBucket")));
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }

        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(TABLES_FILE_PATH));
            tables = (Vector<Table>) oi.readObject();
            oi.close();
        } catch (Exception e) {
            tables = new Vector<>();
        }

        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(NEXT_PAGE_NAME_PATH));
            oi.readInt();
            oi.close();
        } catch (Exception e) {
            try {
                ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(NEXT_PAGE_NAME_PATH));
                os.writeInt(0);
                os.close();
            } catch (Exception e1) {
                System.out.println(e1.getStackTrace());
            }
        }

    }

    private void check(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        for (Table table : tables) {
            if (table.getTableName().equals(tableName)) throw new DBAppException(tableName + " already exists");
        }
        if (!colNameType.containsKey(clusteringKey)) throw new DBAppException("Invalid clusteringKey");
        if ((!colNameType.keySet().equals(colNameMin.keySet())) || (!colNameMin.keySet().equals(colNameMax.keySet())) || (!colNameType.keySet().equals(colNameMax.keySet())))
            throw new DBAppException("Each Column must have corresponding type, min and max");

        for (Map.Entry<String, String> e : colNameType.entrySet()) {
            String strColType = e.getValue();
            boolean f = false;
            f |= strColType.equals("java.lang.Integer");
            f |= strColType.equals("java.lang.String");
            f |= strColType.equals("java.lang.Double");
            f |= strColType.equals("java.util.Date");
            if (!f) throw new DBAppException("Type is not valid");

        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        check(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
        try {
            tables.add(new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax));
//            System.out.println(tableName+" "+clusteringKey+" "+colNameType+" "+colNameMin+" "+colNameMax);
//            System.out.println(tables);
        } catch (Exception e) {
            e.printStackTrace();
        }

        reserialize();
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    public static void changeStringToMyString(Hashtable<String, Object> in) {
        for (Map.Entry<String, Object> e : in.entrySet())
            if (e.getValue() instanceof String)
                e.setValue(new MyString((String) e.getValue()));
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        changeStringToMyString(colNameValue);
        for (Table table : tables)
            if (table.getTableName().equals(tableName)) {
                table.insertTuple(colNameValue);
                return;
            }
        throw new DBAppException("This table doesn't exist");
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        changeStringToMyString(columnNameValue);
        for (Table table : tables)
            if (table.getTableName().equals(tableName)) {
                table.updateTuple(clusteringKeyValue, columnNameValue);
                return;
            }
        throw new DBAppException("This table doesn't exist");
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        changeStringToMyString(columnNameValue);
        for (Table table : tables)
            if (table.getTableName().equals(tableName)) {
                table.deleteTuple(columnNameValue);
                return;
            }
        throw new DBAppException("This table doesn't exist");
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tables.size(); i++) {
            sb.append("Table " + i);
            sb.append(tables.get(i).toString());
            sb.append("\n");
        }
        return sb.toString();
    }

}
