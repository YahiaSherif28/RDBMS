import javax.management.ObjectInstance;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {

    private static final String METADATA_FILE_PATH = "src/main/resources/data/metadata.csv";
    private static final String TABLES_FILE_PATH = "src/main/resources/data/tables/";

    private String tableName;
    transient private Vector<Page> pages;
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
        //   System.out.println(tableName);
    }

    public void writeToMetaDataFile() {
        try {
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(METADATA_FILE_PATH, true)));
            for(int i = 0; i < colNames.size(); i++) {
                String minString = colMin.get(i).toString();
                String maxString = colMax.get(i).toString();
                if(colTypes.get(i).equals("java.util.Date")) {
                    Date minDate = (Date)colMin.get(i);
                    Date maxDate = (Date)colMax.get(i);
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
    }

    public void readFromMetaDataFile() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(METADATA_FILE_PATH));
            while(br.ready()) {
                String[] column = br.readLine().split(",");
                if(!column[0].equals(tableName))
                    continue;
                colNames.add(column[1]);
                colTypes.add(column[2]);
                colNameId.put(column[1], colNames.size() - 1);
                colMin.add(stringToComparable(column[5], column[2]));
                colMax.add(stringToComparable(column[6], column[2]));
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void loadTable() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(TABLES_FILE_PATH + tableName + ".class"));
        pages = (Vector<Page>) oi.readObject();
        indexOfClusteringKey = (Integer) oi.readObject();

        colNames = new Vector<>();
        colNameId = new TreeMap<String, Integer>();
        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();

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
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(TABLES_FILE_PATH + tableName + ".class"));
        os.writeObject(pages);
        os.writeObject(indexOfClusteringKey);
        /*
        os.writeObject(colNames);
        os.writeObject(colNameId);
        os.writeObject(colTypes);
        os.writeObject(colMin);
        os.writeObject(colMax);
        */
        pages = null;
        indexOfClusteringKey = null;
        colNames = null;
        colNameId = null;
        colTypes = null;
        colMin = null;
        colMax = null;

        os.close();
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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
                if (value.compareTo(colMin.get(i)) < 0 || value.compareTo(colMax.get(i)) > 0){
                    //System.out.println(value.compareTo(colMin.get(i)) +" "+value.compareTo(colMax.get(i)) );
                    throw new DBAppException(String.format("The value for column %s is not between the min and the max. %s %s %s", colName, value, colMin.get(i), colMax.get(i)));
            }
                newTupleVector.add(value);
            }
        }

        try {
            add(new Tuple(newTupleVector, indexOfClusteringKey));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            closeTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateTuple(String clusteringKeyValue, Hashtable<String, Object> colNameValue) throws DBAppException {
        try {
            loadTable();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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

    public String getTableName() {
        return tableName;
    }

    public String toString() {
        return pages.toString();
    }

}