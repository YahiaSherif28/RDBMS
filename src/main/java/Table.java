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
    transient private Vector<Boolean> colIndex;

    public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        this.tableName = tableName;
        pages = new Vector<>();
        indices = new Vector<>();

        colNames = new Vector<>();
        for (Map.Entry<String, String> e : colNameType.entrySet()) {
            colNames.add(e.getKey());
        }

        indexOfClusteringKey = colNames.indexOf(clusteringKey);

        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();
        colIndex = new Vector<>();
        for (int i = 0; i < colNames.size(); i++)
            colIndex.add(false);

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
        } catch (Exception e) {
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
                pw.printf("%s,%s,%s,%s,%s,%s,%s\n", tableName, colNames.get(i), colTypes.get(i), indexOfClusteringKey.equals(i) ? "True" : "False",
                        colIndex.get(i).equals(Boolean.TRUE) ? "True" : "False", minString, maxString);
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
        colIndex = null;
    }

    public void readFromMetaDataFile() {
        colNames = new Vector<>();
        colNameId = new TreeMap<String, Integer>();
        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();
        colIndex = new Vector<>();

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
                colIndex.add(column[4].equals("True") ? true : false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateMetaData() {
        Vector<String> metaDataFileContents = new Vector<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(METADATA_FILE_PATH));
            while (br.ready()) {
                String newRow = br.readLine();
                String[] column = newRow.split(",");
                if (!column[0].equals(tableName)) {
                    metaDataFileContents.add(newRow);
                    continue;
                }
                int columnId = colNameId.get(column[1]);
                column[4] = colIndex.get(columnId).equals(Boolean.TRUE) ? "True" : "False";
                metaDataFileContents.add(String.format("%s,%s,%s,%s,%s,%s,%s", column[0], column[1], column[2], column[3], column[4], column[5], column[6]));
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(METADATA_FILE_PATH, false)));
            pw.print("");
            pw.close();
            pw = new PrintWriter(new BufferedWriter(new FileWriter(METADATA_FILE_PATH, true)));
            for(String s : metaDataFileContents)
                pw.println(s);
            pw.flush();
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


    public String getColumnType (String colName) {
        try {
            loadTable();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        String type = colTypes.get(colNameId.get(colName)) ;
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return type ;
    }

    public String getTableName() {
        return tableName;
    }

    public Comparable[][] getColumnRange(String[] columnNames) {
        Comparable[][] minRange = new Comparable[columnNames.length][10];
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            int columnIndex = colNameId.get(columnName);
            String columnType = colTypes.get(columnIndex);
            if (columnType.equals("java.lang.Integer")) {
                Integer min = (Integer) colMin.get(columnIndex);
                Integer max = (Integer) colMax.get(columnIndex);
                int step = (max - min + 1 + 9) / 10;
                minRange[i][0] = min;
                for (int j = 1; j < 10; j++)
                    minRange[i][j] = (Integer) minRange[i][j - 1] + step;
            } else if (columnType.equals("java.lang.Double")) {
                Double min = (Double) colMin.get(columnIndex);
                Double max = (Double) colMax.get(columnIndex);
                double step = (max - min + 1) / 10;
                minRange[i][0] = min;
                for (int j = 1; j < 10; j++)
                    minRange[i][j] = (Double) minRange[i][j - 1] + step;
            } else if (columnType.equals("java.util.Date")) {
                long min = ((Date) colMin.get(columnIndex)).getTime();
                long max = ((Date) colMax.get(columnIndex)).getTime();
                long step = (max - min + 1 + 9) / 10;
                minRange[i][0] = new Date(min);
                for (int j = 1; j < 10; j++)
                    minRange[i][j] = new Date(((Date) minRange[i][j - 1]).getTime() + step);
            } else {
                MyString minString = (MyString) colMin.get(columnIndex);
                MyString maxString = (MyString) colMax.get(columnIndex);
                long min = minString.hashValue();
                long max = maxString.hashValue();
                long step = (max - min + 1 + 9) / 10;
                minRange[i][0] = min;
                for (int j = 1; j < 10; j++)
                    minRange[i][j] = (Long) minRange[i][j - 1] + step;
            }
        }
        return minRange;
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
        Comparable[][] minRange = getColumnRange(columnNames);

        // create index and add to indices vector
        GridIndex newIndex = new GridIndex(columnNames, minRange);
        indices.add(newIndex);

        // populate index
        int[] colIds = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; i++)
            colIds[i] = colNameId.get(columnNames[i]);

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

        // update index boolean
        for (int i : colIds)
            colIndex.set(i, true);

        updateMetaData();

        try {
            closeTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateIndex(String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, DBAppException {

        String type = colTypes.get(indexOfClusteringKey);               // we can use select * where pk = clusteringKeyValue
        Comparable key = stringToComparable(clusteringKeyValue, type);
        Page oldPage = pages.get(binarySearch(key));
        Tuple t = oldPage.getTuple(key);
        if (t == null) {
            return;
        }
        Vector<Comparable> tupleValues = t.getTupleData();

        for (GridIndex index : indices) {                                        // get the involved indices
            Vector<Comparable> oldColumnsValues = new Vector<Comparable>();
            for (String indexColumn : index.getColumns()) {                       // get the old values inserted in the index
                oldColumnsValues.add(tupleValues.get(colNameId.get(indexColumn)));
            }
            index.deleteTuple(oldColumnsValues, oldPage.getFileName());
            Vector<Comparable> newColumnValues = new Vector<Comparable>();
            for (String indexColumn : index.getColumns()) {
                if (columnNameValue.keySet().contains(indexColumn)) {
                    newColumnValues.add((Comparable) columnNameValue.get(indexColumn));
                } else
                    newColumnValues.add(tupleValues.get(colNameId.get(indexColumn)));
            }
            index.insertTuple(newColumnValues, oldPage.getFileName());
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
            populateRowIndex(row, newPage.getFileName());
            return;
        }

        int insertionIndex = binarySearch(row.getPK());
        if (pages.get(insertionIndex).isFull()) {
            pages.get(insertionIndex).add(row);
            Page newPage = pages.get(insertionIndex).split();
            pages.add(insertionIndex + 1, newPage);
            int tupleIndex = pages.get(insertionIndex).searchForTuple(row.getPK());

            String oldPageName = pages.get(insertionIndex).getFileName();
            String newPageName = newPage.getFileName();

            populateRowIndex(row, oldPageName);
            for (Tuple tuple : newPage.loadAndGetData())
                updatePageRowIndex(tuple, oldPageName, newPageName);
        } else {
            Page insertionPage = pages.get(insertionIndex);
            insertionPage.add(row);
            populateRowIndex(row, insertionPage.getFileName());
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
          //  System.out.println(colNames.get(i));
           // System.out.println(this.colNames.get(indexOfClusteringKey));
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
            Tuple newTuple = new Tuple(newTupleVector, indexOfClusteringKey);
            add(newTuple);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updatePageRowIndex(Tuple updatedTuple, String oldPageName, String newPageName) {
        for (GridIndex index : indices) {
            Vector<Comparable> tupleValues = updatedTuple.getTupleData();
            Vector<Comparable> newValues = new Vector<Comparable>();
            for (String indexColumn : index.getColumns()) {
                newValues.add(tupleValues.get(colNameId.get(indexColumn)));
            }
            index.updateTuplePage(newValues, oldPageName, newPageName);
        }
    }

    private void populateRowIndex(Tuple insertTuple, String insertPageName) {
        for (GridIndex index : indices) {
            Vector<Comparable> tupleValues = insertTuple.getTupleData();
            Vector<Comparable> newValues = new Vector<Comparable>();
            for (String indexColumn : index.getColumns()) {
                newValues.add(tupleValues.get(colNameId.get(indexColumn)));
            }
            index.insertTuple(newValues, insertPageName);
        }
    }

    public void updateTuple(String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {
        try {
            loadTable();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            updateIndex(clusteringKeyValue, colNameValue);
        } catch (Exception e) {
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
            Vector<Tuple> deletedTuples = p.deleteTuples(colNameVal);
            deleteFromIndex(p, deletedTuples);
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

    private void deleteFromIndex(Page deletePage, Vector<Tuple> deletedTuples) {
        for (Tuple tuple : deletedTuples) {
            Vector<Comparable> tupleValues = tuple.getTupleData();
            for (GridIndex index : indices) {
                Vector<Comparable> indexValues = new Vector<Comparable>();
                for (String indexColumn : index.getColumns()) {
                    indexValues.add(tupleValues.get(colNameId.get(indexColumn)));
                }
                index.deleteTuple(indexValues, deletePage.getFileName());
            }
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
            if (s._objValue instanceof String)
                s._objValue = new MyString((String) s._objValue);
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
                result.addAll(p.select(sqlTerms, arrayOperators, colNameId));
            }
        }
        closeTable();
        return result.iterator();
    }

    public HashSet<String> selectSegment(Vector<SQLTerm> segment) throws DBAppException, IOException, ClassNotFoundException {
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
            for (String s : bestIndex.getColumns()) {
                if (!range.containsKey(s)) {
                    int colIndex = colNameId.get(s);
                    Comparable min = colMin.get(colIndex);
                    Comparable max = colMax.get(colIndex);
                    range.put(s, new Range(min, max));
                }
            }
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
        } else if (c instanceof MyString) {
            String s = ((MyString) c).toString();
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
            return val;
        }
        throw new DBAppException();
    }
}