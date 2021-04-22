import javax.management.ObjectInstance;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class Table implements Serializable {
    public Vector<Page> pages;

    private String tableName ;
    private Integer indexOfClusteringKey;
    private Vector<String> colNames;
    private Vector<String> colTypes;
    private Vector<Comparable> colMin;
    private Vector<Comparable> colMax;

    public Table(String tableName, String clusteringKey, Hashtable<String, String> colNameType, Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax ) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        this.tableName = tableName;
        pages = new Vector<>();

        colNames = new Vector<>();
        for(Map.Entry<String, String> e : colNameType.entrySet()){
            colNames.add(e.getKey());
        }

        indexOfClusteringKey = colNames.indexOf(clusteringKey);

        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();
        for(String name : colNames){
            colTypes.add(colNameType.get(name));
            String strColType =colNameType.get(name);
            Class myClass = Class.forName(strColType);
            Constructor myConstructor = myClass.getConstructor(String.class);
            Comparable myMin = (Comparable) myConstructor.newInstance(colNameMin.get(name));
            Comparable myMax = (Comparable) myConstructor.newInstance(colNameMax.get(name));
            colMin.add(myMin);
            colMax.add(myMax);
        }
    }

    public void insertTuple(Hashtable<String, Object> colNameValue) throws DBAppException {
        Vector<Comparable> newTupleVector = new Vector();
        for(int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            Comparable value = (Comparable) colNameValue.get(colName);
            if(value == null) {
              if(i == indexOfClusteringKey)
                  throw new DBAppException("No value was inserted for the primary key.");
              else
                  newTupleVector.add(null);
            } else {
                String enteredType = value.getClass().getName();
                if (!colTypes.get(i).equals(enteredType))
                    throw new DBAppException(String.format("The type of column %s is %s, but the entered type is %s.", colName, colTypes.get(i), enteredType));
                if (value.compareTo(colMin.get(i)) < 0 || value.compareTo(colMax.get(i)) > 0)
                    throw new DBAppException(String.format("The value for column %s is not between the min and the max.", colName));
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
    }

    public void add(Tuple row) throws IOException, ClassNotFoundException {
        if(pages.isEmpty()) {
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

    public String getTableName() {
        return tableName;
    }

    public String toString() {
        return pages.toString();
    }
}
