import javax.management.ObjectInstance;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
    private static final String METADATA_FILE_PATH = "src/main/resources/metadata.csv";
    private static final String TABLES_FILE_PATH = "src/main/resources/data/tables/";

    private final String tableName;
    transient private Vector<Page> pages;
    transient private Vector<GridIndex> indices;
    transient private Integer indexOfClusteringKey;
    transient private Vector<String> colNames;
    transient private TreeMap<String, Integer> colNameId;
    transient private Vector<String> colTypes;
    transient private Vector<Comparable> colMin;
    transient private Vector<Comparable> colMax;

    public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        this.tableName = tableName;
        pages = new Vector<>();

        colNames = new Vector<>();
        for (Map.Entry<String, String> e : colNameType.entrySet()) {
            colNames.add(e.getKey());
        }

        indexOfClusteringKey = colNames.indexOf(clusteringKey);

        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();
        colNameId = new TreeMap<>();
        int id = 0;
        for (String name : colNames) {
            String type = colNameType.get(name);
            if (type.equals("java.lang.String"))
                type = "MyString";
            colTypes.add(type);
            colNameId.put(name, id++);
            Comparable myMin = stringToComparable(colNameMin.get(name), type);
            Comparable myMax = stringToComparable(colNameMax.get(name), type);
            colMin.add(myMin);
            colMax.add(myMax);
        }

        writeToMetaDataFile();
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeToMetaDataFile() {
        try {
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(METADATA_FILE_PATH, true)));
            for (int i = 0; i < colNames.size(); i++) {
                String minString = colMin.get(i).toString();
                String maxString = colMax.get(i).toString();
                if (colTypes.get(i).equals("java.util.Date")) {
                    Date minDate = (Date) colMin.get(i);
                    Date maxDate = (Date) colMax.get(i);
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy MM dd");
                    minString = formatter.format(minDate);
                    formatter = new SimpleDateFormat("yyyy MM dd");
                    maxString = formatter.format(maxDate);
                }
                pw.printf("%s,%s,%s,%s,%s,%s,%s\n", tableName, colNames.get(i), colTypes.get(i), (i == indexOfClusteringKey) ? "True" : "False", "False", minString, maxString);
            }
            pw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }

        colNames = null;
        colNameId = null;
        colTypes = null;
        colMin = null;
        colMax = null;
    }

    public void readFromMetaDataFile() {
        colNames = new Vector<>();
        colNameId = new TreeMap<String, Integer>();
        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(METADATA_FILE_PATH));
            while (br.ready()) {
                String[] column = br.readLine().split(",");
                if (!column[0].equals(tableName))
                    continue;
                colNames.add(column[1]);
                colTypes.add(column[2]);
                colNameId.put(column[1], colNames.size() - 1);
                colMin.add(stringToComparable(column[5], column[2]));
                colMax.add(stringToComparable(column[6], column[2]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void loadTable() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(TABLES_FILE_PATH + tableName + ".ser"));
        pages = (Vector<Page>) oi.readObject();
        indices = (Vector<GridIndex>) oi.readObject();
        indexOfClusteringKey = (Integer) oi.readObject();

        readFromMetaDataFile();
        /*
        colNames = (Vector<String>) oi.readObject();
        colNameId = (TreeMap<String, Integer>) oi.readObject();
        colTypes = (Vector<String>) oi.readObject();
        colMin = (Vector<Comparable>) oi.readObject();
        colMax = (Vector<Comparable>) oi.readObject();
        */
        oi.close();
    }

    public void closeTable() throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(TABLES_FILE_PATH + tableName + ".ser"));
        os.writeObject(pages);
        os.writeObject(indices);
        os.writeObject(indexOfClusteringKey);
        /*
        os.writeObject(colNames);
        os.writeObject(colNameId);
        os.writeObject(colTypes);
        os.writeObject(colMin);
        os.writeObject(colMax);
        */
        pages = null;
        indices = null;
        indexOfClusteringKey = null;

        os.close();
    }

    public String getTableName() {
        return tableName;
    }

    public void createIndex(String[] columnNames) throws DBAppException {
        try {
            loadTable();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // check if column names exist
        boolean columnsExist = true;
        for (String column : columnNames) {
            if (!colNameId.containsKey(column))
                throw new DBAppException(String.format("Column %s doesn't exist in the table %s.", column, tableName));
        }

        // check if index is repeated
        Arrays.sort(columnNames);
        for (GridIndex index : indices) {
            if (index.equals(columnNames))
                throw new DBAppException("Index already Exists");
        }

        // generate range values for all columns
        Comparable[][] minRange = null;
        Comparable[][] maxRange = null;

        // create index and add to indices vector
        GridIndex newIndex = new GridIndex(columnNames, minRange, maxRange);
        indices.add(newIndex);

        // populate index
        int[] colIds = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; i++)
            colIds[i] = colNameId.get(columnNames);

        for (Page page : pages) {
            Vector<Tuple> rows = page.loadAndGetData();
            for (Tuple row : rows) {
                Vector<Comparable> rowData = row.getTupleData();
                Vector<Comparable> values = new Vector<>();
                for (int i = 0; i < columnNames.length; i++)
                    values.add(rowData.get(colIds[i]));
                newIndex.insertTuple(values, page.getFileName());
            }
        }

        try {
            closeTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateIndex (String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String type = colTypes.get(indexOfClusteringKey);               // we can use select * where pk = clusteringKeyValue
        Comparable key = stringToComparable(clusteringKeyValue, type);
        Page oldPage = pages.get(binarySearch(key)) ;
        Vector<Comparable> tupleValues = oldPage.getTuple(key).getTupleData() ;

        for (GridIndex index : indices ){                                        // get the involved indices
            Vector<Comparable> oldColumnsValues = new Vector<Comparable>() ;
             for (String indexColumn : index.getColumns()) {                       // get the old values inserted in the index
                 oldColumnsValues.add(tupleValues.get(colNameId.get(indexColumn))) ;
            }
             index.deleteTuple(oldColumnsValues , oldPage.getFileName());
             Vector<Comparable> newColumnValues = new Vector<Comparable>() ;
             for (String indexColumn : index.getColumns() ) {
                 if (columnNameValue.keySet().contains(indexColumn)) {
                     newColumnValues.add((Comparable) columnNameValue.get(indexColumn)) ;
                 }
                 else
                     newColumnValues.add(tupleValues.get(colNameId.get(indexColumn))) ;
             }
             index.insertTuple(newColumnValues,oldPage.getFileName());
        }
    }



    private int binarySearch(Comparable key) {
        int lo = 0;
        int hi = pages.size() - 1;
        int ans = pages.size();
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (pages.get(mid).getMinPK().compareTo(key) > 0) {
                ans = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return Math.max(ans - 1, 0);
    }

    public void add(Tuple row) throws IOException, ClassNotFoundException, DBAppException {
        if (pages.isEmpty()) {
            Page newPage = new Page(DBApp.getNextPageName());
            newPage.getData().add(row);
            newPage.closePage();
            pages.add(newPage);
            return;
        }
        int insertionIndex = binarySearch(row.getPK());
        if (pages.get(insertionIndex).isFull()) {
            pages.get(insertionIndex).add(row);
            Page newPage = pages.get(insertionIndex).split();
            pages.add(insertionIndex + 1, newPage);
        } else {
            pages.get(insertionIndex).add(row);
        }
    }

    public void insertTuple(Hashtable<String, Object> colNameValue) throws DBAppException {
        try {
            loadTable();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        Vector<Comparable> newTupleVector = new Vector();
        for (int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            Comparable value = (Comparable) colNameValue.get(colName);
            if (value == null) {
                if (i == indexOfClusteringKey)
                    throw new DBAppException("No value was inserted for the primary key.");
                else
                    newTupleVector.add(null);
            } else {
                String enteredType = value.getClass().getName();
                if (!colTypes.get(i).equals(enteredType))
                    throw new DBAppException(String.format("The type of column %s is %s, but the entered type is %s.", colName, colTypes.get(i), enteredType));
                if (value.compareTo(colMin.get(i)) < 0 || value.compareTo(colMax.get(i)) > 0) {
                    //System.out.println(value.compareTo(colMin.get(i)) +" "+value.compareTo(colMax.get(i)) );
                    throw new DBAppException(String.format("The value for column %s is not between the min and the max. %s %s %s", colName, value, colMin.get(i), colMax.get(i)));
                }
                newTupleVector.add(value);
            }
        }


        try {
            add(new Tuple(newTupleVector, indexOfClusteringKey));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void updateTuple(String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException{
        try {
            loadTable();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            updateIndex(clusteringKeyValue, colNameValue) ;
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        String type = colTypes.get(indexOfClusteringKey);

        Comparable key = null;

        try {
            key = stringToComparable(clusteringKeyValue, type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (pages.isEmpty())
            return;
        int pageIndex = binarySearch(key);
        Page p = pages.get(pageIndex);
        Hashtable<Integer, Comparable> colNameVal = new Hashtable<Integer, Comparable>();
        for (Map.Entry e : colNameValue.entrySet()) {
            if (!colNameId.containsKey(e.getKey()))
                throw new DBAppException(String.format("Update Failed. Column %s doesn't exist", e.getKey()));
            int id = colNameId.get(e.getKey());
            Comparable val = (Comparable) e.getValue();
            Comparable min = colMin.get(id);
            Comparable max = colMax.get(id);
            if (val.compareTo(min) < 0 || val.compareTo(max) > 0) {
                // System.out.println(val.compareTo(min) + " " + val.compareTo(max));
                //throw new DBAppException(String.format("Update Failed. Column %s has min Value %s and max Value %s. Can't update value to %s", e.getKey(), min.toString(), max.toString(), val.toString()));
            }
            colNameVal.put(id, val);
        }
        p.update(key, colNameVal);
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTuple(Hashtable<String, Object> columnNameValue) {
        try {
            loadTable();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        Hashtable<Integer, Comparable> colNameVal = new Hashtable<Integer, Comparable>();
        for (Map.Entry e : columnNameValue.entrySet()) {
            int id = colNameId.get(e.getKey());
            Comparable val = (Comparable) e.getValue();
            colNameVal.put(id, val);
        }
        Vector<Page> newPages = new Vector<>();
        for (Page p : pages) {
            p.deleteTuples(colNameVal);
            if (!p.isEmpty()) {
                newPages.add(p);
            }
        }
        pages = newPages;
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Date stringToDate(String s) {
        int year = Integer.parseInt(s.trim().substring(0, 4));
        int month = Integer.parseInt(s.trim().substring(5, 7));
        int day = Integer.parseInt(s.trim().substring(8));
        return new Date(year - 1900, month - 1, day);
    }

    public static Comparable stringToComparable(String object, String type) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        if (type.equals("java.util.Date")) {
            return stringToDate(object);
        } else {
            Class myClass = Class.forName(type);
            Constructor myConstructor = myClass.getConstructor(String.class);
            return (Comparable) myConstructor.newInstance(object);
        }
    }

    public String toString() {
        try {
            loadTable();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(tableName + "\n");
        for (int i = 0; i < pages.size(); i++) {
            sb.append("Page " + i);
            sb.append(pages.get(i).toString());
            sb.append("\n");
        }
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static final String AND = "AND", OR = "OR", XOR = "XOR";

    public Iterator select(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException, IOException, ClassNotFoundException {
        loadTable();
        for (SQLTerm s : sqlTerms) {
            if (!colNames.contains(s._strColumnName)) {
                throw new DBAppException("Table doesn't contain this column");
            }
        }
        Vector<SQLTerm> segment = new Vector();
        HashSet<String> pagesToOpen = new HashSet<>();
        for (int i = 0; i < sqlTerms.length; i++) {
            segment.add(sqlTerms[i]);
            if (i == arrayOperators.length || !arrayOperators[i].equals(AND)) {
                pagesToOpen.addAll(selectSegment(segment));
                segment = new Vector<>();
            }
        }
        Vector<Tuple> result = new Vector<>();
        for (Page p : pages) {
            if (pagesToOpen.contains(p.getFileName())) {
                result.addAll(p.select(sqlTerms, arrayOperators));
            }
        }
        closeTable();
        return result.iterator();
    }

    public HashSet<String> selectSegment(Vector<SQLTerm> segment) throws DBAppException {
        Hashtable<String, Range> range = new Hashtable<>();
        for (SQLTerm sqlTerm : segment) {
            int indexOfCol = colNames.indexOf(sqlTerm._strColumnName);
            Range myNewRange = range.getOrDefault(sqlTerm._strColumnName,
                    new Range(colMin.get(indexOfCol), colMax.get(indexOfCol))).intersect(sqlTerm);
            if (myNewRange.isDegenerate()) {
                return new HashSet<>();
            }
            range.put(sqlTerm._strColumnName, myNewRange);
        }

        GridIndex bestIndex = pickBestIndex(range);
        if (bestIndex == null) {
            return selectWithoutIndex(range);
        } else {
            return bestIndex.select(range);
        }
    }


    private HashSet<String> selectWithoutIndex(Hashtable<String, Range> range) {
        HashSet<String> pagesToOpen = new HashSet<>();
        String clusteringKey = colNames.get(indexOfClusteringKey);
        Range myRange = range.getOrDefault(clusteringKey, new Range(colMin.get(indexOfClusteringKey), colMax.get(indexOfClusteringKey)));
        for (Page p : pages) {
            if (p.getMinPK().compareTo(myRange.min) >= 0) {
                pagesToOpen.add(p.getFileName());
            } else if (p.getMinPK().compareTo(myRange.max) > 0) {
                break;
            }
        }
        return pagesToOpen;
    }

    private GridIndex pickBestIndex(Hashtable<String, Range> range) throws DBAppException {
        GridIndex bestIndex = null;
        double bestScore = 1 - 1e-9;
        for (GridIndex curIndex : indices) {
            double curScore = getScoreOfIndex(range, curIndex);
            if (curScore < bestScore) {
                bestIndex = curIndex;
                bestScore = curScore;
            }
        }
        return bestIndex;
    }

    private double getScoreOfIndex(Hashtable<String, Range> range, GridIndex index) throws DBAppException {
        double score = 1;
        for (String colName : index.getColumns()) {
            int indexOfCol = colNames.indexOf(colName);
            Range myRange = range.get(colName);
            if (myRange == null) {
                continue;
            }
            double numerator = getNumericValue(myRange.max) - getNumericValue(myRange.min) + 1;
            double denominator = getNumericValue(colMax.get(indexOfCol)) - getNumericValue(colMin.get(indexOfCol)) + 1;
            score *= numerator / denominator;
        }
        return score;
    }

    private double getNumericValue(Comparable c) throws DBAppException {
        if (c instanceof Integer || c instanceof Double) {
            return (double) c;
        } else if (c instanceof Date) {
            return ((Date) c).getTime();
        } else if (c instanceof String) {
            String s = (String) c;
            double val = 0;
            double mult = 1;
            for (int i = 0; i < 5; i++) {
                if (4 - i >= s.length()) {
                    val += 0;
                } else {
                    val += mult * (s.charAt(4 - i) - 'a' + 1);
                }
                mult *= 27;
            }
        }
        throw new DBAppException();
    }
}