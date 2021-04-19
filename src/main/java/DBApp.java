import java.io.*;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static final String NEXT_PAGE_NAME_FILE = "nextPageName.class";
    private static final String PAGES_FILE_PATH = "src/main/data/";
    private static final String TABLES_FILE = "tables.class";
    private Vector<Table> tables;

    public static void main(String[] args) throws Exception {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream("v1.class"));
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("v1.class"));
        Vector<Integer> v = new Vector();
        for (int i = 0; i < 10; i++) {
            v.add(i);
        }
        os.writeObject(v);
        System.out.println(oi.readObject());
    }

    private static int getNextPageNameAsInt() throws IOException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(NEXT_PAGE_NAME_FILE));
        Integer nextPageName = oi.readInt();
        nextPageName++;
        oi.close();
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(NEXT_PAGE_NAME_FILE));
        os.writeInt(nextPageName);
        os.close();
        return nextPageName;
    }

    public static String getNextPageName() throws IOException {
        return PAGES_FILE_PATH + String.valueOf(getNextPageNameAsInt()) + ".class";
    }

    @Override
    public void init() {
        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(PAGES_FILE_PATH + TABLES_FILE));
            tables = (Vector<Table>) oi.readObject();
            oi.close();
        } catch (Exception e) {
            tables = new Vector<>();
        }
        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(PAGES_FILE_PATH + NEXT_PAGE_NAME_FILE));
            oi.readInt();
            oi.close();
        } catch (Exception e) {
            try {
                ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(PAGES_FILE_PATH + NEXT_PAGE_NAME_FILE));
                os.writeInt(0);
                os.close();
            } catch (Exception e1) {

            }
        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {

    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }
}
