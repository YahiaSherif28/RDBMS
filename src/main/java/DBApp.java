import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.sql.Array;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
        Scanner sc = new Scanner(System.in);

        while (sc.hasNext()) {
            sb.append(sc.nextLine());
        }
        //sb.append("CREATE TABLE students (id int PRIMARY KEY , name varchar , gpa double )");
        //sb.append("INSERT INTO students (id , name ,gpa) VALUES (18829 , Khater ,0.5 )");
       // sb.append("DELETE from students WHERE name = Khater AND gpa = 0.5 AND id = 18829") ;
       // sb.append("SELECT * from students WHERE gpa >= 0.5 AND id >= 16076 ") ;}
        try {
            Iterator ans = db.parseSQL(sb);
            if(ans==null) System.out.println("NOT SELECTION");
            else {
                while (ans.hasNext()){
                    System.out.println(ans.next());
                }
            }
       //     System.out.println(ans==null?ans:ans.next());
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
        //System.out.println(Arrays.toString(sqLiteParser.getRuleNames()));
        // Walk the `select_stmt` production and listen when the parser
        // enters the `expr` production.
        final Iterator[] ret = {null};
        final List<String> functionNames = new ArrayList<String>();

        ParseTreeWalker.DEFAULT.walk(new SQLiteParserBaseListener() {

            @Override
            public void enterSql_stmt(SQLiteParser.Sql_stmtContext ctx) {

            }

            @Override
            public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                String tableName = ctx.select_core(0).table_or_subquery(0).table_name().getText() ;
              //  System.out.println(ctx.select_core().get(0).expr().get(0).expr().get(0).ASSIGN());
                int size = ctx.select_core().get(0).expr().get(0).expr().size();

                Vector<SQLTerm> ttmp  = new Vector<>();
                Vector<String> otmp = new Vector<>();

                    SQLiteParser.ExprContext expression = ctx.select_core().get(0).expr().get(0);
                    //  System.out.println(ctx.select_core().get(0).expr().get(0).getText());
                    while (expression.AND_()!=null || expression.OR_()!=null) {
                        String colName = expression.expr().get(1).expr().get(0).getText();
                        String value = expression.expr().get(1).expr().get(1).getText();
                        String operand = "";
                        if (expression.expr().get(1).ASSIGN() != null) operand = "=";
                        if (expression.expr().get(1).GT_EQ() != null) operand = ">=";
                        if (expression.expr().get(1).GT() != null) operand = ">";
                        if (expression.expr().get(1).LT_EQ() != null) operand = "<=";
                        if (expression.expr().get(1).LT() != null) operand = "<";
                        if (expression.expr().get(1).NOT_EQ1() != null) operand = "!=";
                        SQLTerm sqlTerm = new SQLTerm();
                        sqlTerm._strTableName = tableName;
                        sqlTerm._strColumnName = colName;
                        sqlTerm._strOperator = operand;
                        sqlTerm._objValue = getObject(tableName,colName,value);
                        ttmp.add(sqlTerm);
                        String op = expression.AND_()==null?"OR":"AND";
                        otmp.add(op);
                        expression = expression.expr().get(0);
                    //    System.out.println(colName+" "+operand+" "+ value);

                    }
                String colName = expression.expr().get(0).getText();
                String value = expression.expr().get(1).getText();
                String operand = "";
                if (expression.ASSIGN() != null) operand = "=";
                if (expression.GT_EQ() != null) operand = ">=";
                if (expression.GT() != null) operand = ">";
                if (expression.LT_EQ() != null) operand = "<=";
                if (expression.LT() != null) operand = "<";
                if (expression.NOT_EQ1() != null) operand = "!=";
                SQLTerm sqlTerm = new SQLTerm();
                sqlTerm._strTableName = tableName;
                sqlTerm._strColumnName = colName;
                sqlTerm._strOperator = operand;
                sqlTerm._objValue = getObject(tableName,colName,value);
                ttmp.add(sqlTerm);
              //  System.out.println(colName+" "+operand+" "+ value);
                SQLTerm[] sqlTerms = new SQLTerm[ttmp.size()];
                String[] arrayOperators = new String[otmp.size()];
                for (int i = ttmp.size()-1; i>=0; i--)sqlTerms[ttmp.size()-1-i] = ttmp.get(i);
                for (int i = otmp.size()-1; i>=0; i--)arrayOperators[otmp.size()-1-i] = otmp.get(i);

//                System.out.println(Arrays.toString(sqlTerms));
//                System.out.println(Arrays.toString(arrayOperators));
                try {
                    ret[0] = selectFromTable(sqlTerms,arrayOperators);
                } catch (DBAppException e) {
                    e.printStackTrace();
                }

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
                       // System.out.println(x.column_constraint().get(0).getText());
                        if(x.column_constraint().get(0).getText().toLowerCase().equals("primarykey")) id = x.column_name().getText();
                    }
                    columnNamesAndTypes.put(x.column_name().getText(),x.type_name().getText());
                }
                Hashtable<String,String> minValues = new Hashtable<>();
                Hashtable<String,String> maxValues = new Hashtable<>();
                for (Map.Entry<String,String> r : columnNamesAndTypes.entrySet()){
                    minValues.put(r.getKey(),getMin(r.getValue().toLowerCase()));
                    maxValues.put(r.getKey(),getMax(r.getValue().toLowerCase()));
                    columnNamesAndTypes.put(r.getKey(),getType(r.getValue().toLowerCase()));
                }

                try {
                    //System.out.println(tableName+" "+id+" "+columnNamesAndTypes+" "+maxValues+" "+maxValues);
                    createTable(tableName,id,columnNamesAndTypes,minValues,maxValues);
                } catch (DBAppException e) {
                    e.printStackTrace();
                }
            }


            @Override
            public void enterInsert_stmt(SQLiteParser.Insert_stmtContext ctx){
                String tableName = ctx.table_name().getText() ;
                List<SQLiteParser.Column_nameContext> columnsToInsert = ctx.column_name() ;
                List<SQLiteParser.ExprContext> insertedValue = ctx.expr() ;

                Table toInsertIn = getTable(tableName);

                if (toInsertIn!=null) {
                    Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
                    for (int i = 0; i < columnsToInsert.size(); i++) {
                        String col = columnsToInsert.get(i).getText();
                        String type = toInsertIn.getColumnType(col) ;
                        switch (type) {
                            case "MyString" : colNameValue.put(col,insertedValue.get(i).getText());break;
                            case "java.lang.Integer": colNameValue.put(col,Integer.parseInt(insertedValue.get(i).getText()));break;
                            case "java.lang.Double" : colNameValue.put(col,Double.parseDouble(insertedValue.get(i).getText()));break;
                            case "java.util.Date"   : String date = insertedValue.get(i).getText() ;
                                int year = Integer.parseInt(date.trim().substring(0, 4));
                                int month = Integer.parseInt(date.trim().substring(5, 7));
                                int day = Integer.parseInt(date.trim().substring(8));
                                Date dt = new Date(year, month, day);
                                colNameValue.put(col,dt) ;
                                break;
                        }
                       // System.out.println(type);
                    }
                    //System.out.println(colNameValue);
                    try {
                        insertIntoTable(tableName,colNameValue);
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }
                }
                else
                    try {
                        throw new DBAppException("This table doesn't exist");
                    } catch (DBAppException e) {
                        e.printStackTrace();
                    }


            }

            @Override
            public void enterDelete_stmt(SQLiteParser.Delete_stmtContext ctx) {
                String tableName = ctx.qualified_table_name().getText();
                String expr=ctx.expr().getText();
               // System.out.println(ctx.expr());
                ArrayList<String> al=new ArrayList<String>();
                al=inOrderTraversal(ctx.expr(),al);
                Hashtable<String,Object> columnNameValue=new Hashtable<>();
                for (String x:al){
                    if(x.contains("=")) {
                        columnNameValue.put(x.split("=")[0], getObject(tableName,x.split("=")[0],x.split("=")[1]));
                        //  System.out.println(x.split("=")[0]+ " "+ x.split("=")[1]);
                    }
                    else
                        System.out.println("Wrong syntax");
                }
                try {
                    deleteFromTable(tableName,columnNameValue);
                } catch (DBAppException e) {
                    e.printStackTrace();
                }
            }


        }, tree);
     //   System.out.println(ret[0].next());
        return ret[0];
    }

    public ArrayList<String> inOrderTraversal(SQLiteParser.ExprContext expr,ArrayList<String> s) {
        if (expr.expr(0) != null) {
            inOrderTraversal(expr.expr(0),s);

            if(expr.expr(0).expr(0)==null) {
                s.add(expr.getText());
            }
            inOrderTraversal(expr.expr(1),s);
        }
        return s;
    }
    public Object getObject(String tableName,String colName,String value){
        Table t = getTable(tableName);
        if (t==null) return null;
        String type = t.getColumnType(colName);
        Object ret = null;
        switch (type) {
            case "MyString" : ret = new MyString(value);break;
            case "java.lang.Integer": ret = Integer.parseInt(value);break;
            case "java.lang.Double" :ret = Double.parseDouble(value);break;
            case "java.util.Date"   : String date = value ;
                int year = Integer.parseInt(date.trim().substring(0, 4));
                int month = Integer.parseInt(date.trim().substring(5, 7));
                int day = Integer.parseInt(date.trim().substring(8));
                Date dt = new Date(year, month, day);
                ret =dt ;
                break;
        }
        return ret;
    }

    public Table getTable (String tableName) {
        for (Table table : tables )
            if (table.getTableName().equals(tableName))
                return table ;
        return null ;
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
        if(type.contains("varchar")) return "java.lang.String";
        else if(type.equals("int")) return "java.lang.Integer";
        else if(type.equals("double")) return "java.lang.Double";
        else return "java.util.Date";
    }
    public String getMin(String type){
        DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy G HH:mm:ss Z");
        if(type.contains("varchar")) return "AAAAAAA";
        else if(type.equals("int")) return Integer.MIN_VALUE+"";
        else if(type.equals("double")) return Double.MIN_VALUE+"";
        else return df.format(new Date(Long.MIN_VALUE));
    }
    public String getMax(String type){
        DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy G HH:mm:ss Z");
        if(type.contains("varchar")) return "ZZZZZZZ";
        else if(type.equals("int")) return Integer.MAX_VALUE+"";
        else if(type.equals("double")) return Double.MAX_VALUE+"";
        else return df.format(new Date(Long.MAX_VALUE));
    }

}
