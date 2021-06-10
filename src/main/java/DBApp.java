import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class DBApp implements DBAppInterface {
    private static final String NEXT_PAGE_NAME_PATH = "src/main/resources/data/nextPageName.ser";
    private static final String NEXT_BUCKET_NAME_PATH = "src/main/resources/data/nextBucketName.ser";
    private static final String TABLES_FILE_PATH = "src/main/resources/data/tables.ser";
    private static final String PAGES_FILE_PATH = "src/main/resources/data/pages/";
    private static final String BUCKETS_FILE_PATH = "src/main/resources/data/buckets/";
    private static final String RESOURCES_PATH = "src/main/resources/";
    private static final String CONFIG_FILE = "DBApp.config";

    private static Integer MaximumRowsCountinTablePage;
    private static Integer MaximumKeysCountinIndexBucket;

    private Vector<Table> tables;

    public DBApp() {
        //init();
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
        return PAGES_FILE_PATH + String.valueOf(getNextPageNameAsInt()) + ".ser";
    }

    private static int getNextBucketNameAsInt() throws IOException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(NEXT_BUCKET_NAME_PATH));
        Integer nextBucketName = oi.readInt();
        nextBucketName++;
        oi.close();
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(NEXT_BUCKET_NAME_PATH));
        os.writeInt(nextBucketName);
        os.close();
        return nextBucketName;
    }

    public static String getNextBucketName() throws IOException {
        return BUCKETS_FILE_PATH + String.valueOf(getNextBucketNameAsInt()) + ".ser";
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

        try {
            ObjectInputStream oi = new ObjectInputStream(new FileInputStream(NEXT_BUCKET_NAME_PATH));
            oi.readInt();
            oi.close();
        } catch (Exception e) {
            try {
                ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(NEXT_BUCKET_NAME_PATH));
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
        for (Table table : tables)
            if (table.getTableName().equals(tableName)) {
                table.createIndex(columnNames);
                return;
            }
        throw new DBAppException("This table doesn't exist");
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
        if (sqlTerms.length == 0) {
            throw new DBAppException("Invalid expression");
        }
        if (arrayOperators.length != sqlTerms.length - 1) {
            throw new DBAppException("Invalid expression");
        }
        for (int i = 0; i < sqlTerms.length - 1; i++) {
            if (!sqlTerms[i]._strTableName.equals(sqlTerms[i + 1]._strTableName)) {
                throw new DBAppException("Columns from different tables are not valid");
            }
        }
        for (int i = 0; i < arrayOperators.length; i++) {
            if (!arrayOperators[i].equals(Table.AND) && !arrayOperators[i].equals(Table.OR) && !arrayOperators[i].equals(Table.XOR)) {
                throw new DBAppException("Invalid operator");
            }
        }
        String tableName = sqlTerms[0]._strTableName;
        try {
            for (Table t : tables) {
                if (t.getTableName().equals(tableName)) {
                    return t.select(sqlTerms, arrayOperators);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException("Something went wrong while selecting");
        }
        throw new DBAppException("Table not found");
    }

    public static void main(String[] args) throws IOException {
        DBApp db = new DBApp();
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE INDEX X ON A (B,C,D)");
        try {
            db.parseSQL(sb);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    public Iterator parseSQL(StringBuffer strbufSQL) throws
            DBAppException {

        CharStream charStream = CharStreams.fromString(strbufSQL.toString());

        SQLiteLexer sqLiteLexer = new SQLiteLexer(charStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqLiteLexer);
        SQLiteParser sqLiteParser = new SQLiteParser(commonTokenStream);


        ParseTree tree = sqLiteParser.parse();  /// try to change the .select_stmt() method to figure what other methods sqLiteParser contains
        System.out.println(Arrays.toString(sqLiteParser.getRuleNames()));
        // Walk the `select_stmt` production and listen when the parser
        // enters the `expr` production.

        final List<String> functionNames = new ArrayList<String>();

        ParseTreeWalker.DEFAULT.walk(new SQLiteParserBaseListener() {

            @Override
            public void enterSql_stmt(SQLiteParser.Sql_stmtContext ctx) {

            }

            @Override
            public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                System.out.println(ctx.select_core(0).table_or_subquery(0).table_name().getText());
                System.out.println(ctx.select_core(0).result_column(0).expr());
            }

            @Override
            public void enterCreate_index_stmt(SQLiteParser.Create_index_stmtContext ctx) {
                String tableName = ctx.table_name().getText();
                String[] columnNames = new String[ctx.indexed_column().size()];
                int idx = 0;
                for (var x : ctx.indexed_column()) {
                    columnNames[idx++] = x.getText();
                }

                try {
                    createIndex(tableName, columnNames);
                } catch (DBAppException e) {
                    e.printStackTrace();
                }
            }
             @Override
            public void enterCreate_table_stmt(SQLiteParser.Create_table_stmtContext ctx) {
                String tableName = ctx.table_name().getText();
                Hashtable<String,String> columnNamesAndTypes = new Hashtable<>();

                int idx = 0;
                String id = "";
                for (var x : ctx.column_def()) {
                    if(x.column_constraint().size()>0){
                        if(x.column_constraint().get(0).getText().equals("PrimaryKey")) id = x.column_name().getText();
                    }
                    columnNamesAndTypes.put(x.column_name().getText(),x.type_name().getText());
                }
                Hashtable<String,String> minValues = new Hashtable<>();
                Hashtable<String,String> maxValues = new Hashtable<>();
                for (Map.Entry<String,String> r : columnNamesAndTypes.entrySet()){
                    minValues.put(r.getKey(),getMin(r.getValue()));
                    maxValues.put(r.getKey(),getMax(r.getValue()));
                    columnNamesAndTypes.put(r.getKey(),getType(r.getValue()));
                }

                try {
                   // System.out.println(tableName+" "+id+" "+columnNamesAndTypes+" "+maxValues+" "+maxValues);
                    createTable(tableName,id,columnNamesAndTypes,minValues,maxValues);
                } catch (DBAppException e) {
                    e.printStackTrace();
                }
            }

        }, tree);
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
      public String getType(String type){
        if(type.equals("varchar")) return "java.lang.String";
        else if(type.equals("int")) return "java.lang.Integer";
        else if(type.equals("double")) return "java.lang.Double";
        else return "java.util.Date";
    }
    public String getMin(String type){
        DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy G HH:mm:ss Z");
        if(type.equals("varchar")) return "AAAAAAA";
        else if(type.equals("int")) return Integer.MIN_VALUE+"";
        else if(type.equals("double")) return Double.MIN_VALUE+"";
        else return df.format(new Date(Long.MIN_VALUE));
    }
    public String getMax(String type){
        DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy G HH:mm:ss Z");
        if(type.equals("varchar")) return "ZZZZZZZ";
        else if(type.equals("int")) return Integer.MAX_VALUE+"";
        else if(type.equals("double")) return Double.MAX_VALUE+"";
        else return df.format(new Date(Long.MAX_VALUE));
    }

}
