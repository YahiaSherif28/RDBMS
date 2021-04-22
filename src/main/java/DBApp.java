import javax.lang.model.element.Name;
import java.io.*;
import java.lang.reflect.Constructor;
import java.net.StandardSocketOptions;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static final String NEXT_PAGE_NAME_FILE = "nextPageName.class";
    private static final String PAGES_FILE_PATH = "src/main/data/";
    private static final String RESOURCES_PATH = "src/main/resources/";
    private static final String TABLES_FILE = "tables.class";
    private static final String CONFIG_FILE = "DBApp.config";
    private static Integer MaximumRowsCountinTablePage;
    private static Integer MaximumKeysCountinIndexBucket;

    private Vector<Table> tables;

    public static void main(String[] args) throws Exception {
        DBApp dbapp = new DBApp();
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        Hashtable htblColNameMin = new Hashtable( );
        htblColNameMin.put("id", "1000");
        htblColNameMin.put("name", "A");
        htblColNameMin.put("gpa", "0.7");
        Hashtable htblColNameMax = new Hashtable( );
        htblColNameMax.put("id", "2000");
        htblColNameMax.put("name", "Z");
        htblColNameMax.put("gpa", "2");
        dbapp.createTable( strTableName, "id", htblColNameType ,htblColNameMin,htblColNameMax);

        Hashtable htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", new Integer( 1000 ));
        htblColNameValue.put("name", new String("A" ) );
        htblColNameValue.put("gpa", new Double( 0.95 ) );
        dbapp.insertIntoTable( strTableName , htblColNameValue );

        System.out.println(dbapp.tables.get(0).toString());
    }

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
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(RESOURCES_PATH + NEXT_PAGE_NAME_FILE));
        Integer nextPageName = oi.readInt();
        nextPageName++;
        oi.close();
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(RESOURCES_PATH + NEXT_PAGE_NAME_FILE));
        os.writeInt(nextPageName);
        os.close();
        return nextPageName;
    }

    public static String getNextPageName() throws IOException {
        return PAGES_FILE_PATH + String.valueOf(getNextPageNameAsInt()) + ".class";
    }

    @Override
    public void init() {

        try{
            Properties p = new Properties();
            p.load(new FileReader(RESOURCES_PATH + CONFIG_FILE));
            MaximumRowsCountinTablePage = Integer.parseInt(p.getProperty(new String("MaximumRowsCountinPage")));
            MaximumKeysCountinIndexBucket = Integer.parseInt(p.getProperty(new String("MaximumKeysCountinIndexBucket")));
        }catch (Exception e){
            System.out.println(e.getStackTrace());
        }


        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(RESOURCES_PATH + TABLES_FILE));
            tables = (Vector<Table>) oi.readObject();
            oi.close();
        } catch (Exception e) {
            tables = new Vector<>();
        }
        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(RESOURCES_PATH + NEXT_PAGE_NAME_FILE));
            oi.readInt();
            oi.close();
        } catch (Exception e) {
            try {
                ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(RESOURCES_PATH + NEXT_PAGE_NAME_FILE));
                os.writeInt(0);
                os.close();
            } catch (Exception e1) {

            }
        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        check(tableName,clusteringKey,colNameType,colNameMin,colNameMax);
        try {
            tables.add(new Table(tableName,clusteringKey,colNameType,colNameMin,colNameMax));}
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void check(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        for (Table table : tables){
            if(table.getTableName().equals(tableName)) throw new DBAppException( tableName +" already exists") ;
        }
        if(!colNameType.containsKey(clusteringKey))  throw new DBAppException( "Invalid clusteringKey");
        if((!colNameType.keySet().equals(colNameMin.keySet()))||(!colNameMin.keySet().equals(colNameMax.keySet()))||(!colNameType.keySet().equals(colNameMax.keySet())) ) throw new DBAppException("Each Column must have corresponding type, min and max");

        for(Map.Entry<String,String> e : colNameType.entrySet()){
            String strColType =e.getValue();
            boolean f = false;
            f |= strColType.equals("java.lang.Integer");
            f |= strColType.equals("java.lang.String");
            f |= strColType.equals("java.lang.Double");
            f |= strColType.equals("java.util.Date");
            if(!f) throw new DBAppException("Type is not valid");

        }

    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {

    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        for (Table table : tables)
            if(table.getTableName().equals(tableName)) {
                table.insertTuple(colNameValue);
                return;
            }
        throw new DBAppException("This table doesn't exist");
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
