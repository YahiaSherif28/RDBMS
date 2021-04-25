import javax.management.ObjectInstance;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.util.*;

public class Table implements Serializable {
    public Vector<Page> pages;

    private String tableName ;
    private Integer indexOfClusteringKey;
    private Vector<String> colNames;
    private TreeMap<String,Integer> colNameId;
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
        colNameId = new TreeMap<>();
        int id =0;
        for(String name : colNames){
            colTypes.add(colNameType.get(name));
            colNameId.put(name,id++);
            String strColType =colNameType.get(name);
            Class myClass = Class.forName(strColType);
            Constructor myConstructor = myClass.getConstructor(String.class);
            Comparable myMin = null;
            Comparable myMax = null;
            if(strColType.equals("java.util.Date")){
                String min = colNameMin.get(name);
                int year = Integer.parseInt(min.trim().substring(0, 4));
                int month = Integer.parseInt(min.trim().substring(5, 7));
                int day = Integer.parseInt(min.trim().substring(8));

                Date mindate = new Date(year - 1900, month - 1, day);

                String max = colNameMin.get(name);
                 year = Integer.parseInt(max.trim().substring(0, 4));
                 month = Integer.parseInt(max.trim().substring(5, 7));
                 day = Integer.parseInt(max.trim().substring(8));

                Date maxdate = new Date(year - 1900, month - 1, day);
                 myMin = mindate;
                 myMax = mindate;

            }else{
             myMin = (Comparable) myConstructor.newInstance(colNameMin.get(name));
             myMax = (Comparable) myConstructor.newInstance(colNameMax.get(name));}
            colMin.add(myMin);
            colMax.add(myMax);
        }
     //   System.out.println(tableName);
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

    public void updateTuple (String clusteringKeyValue , Hashtable<String, Object> colNameValue) {
        String type = colTypes.get(indexOfClusteringKey) ;

        Class myClass = null;
        Constructor myConstructor = null ;
        Comparable key = null ;

        try {
            myClass = Class.forName(type);
            myConstructor = myClass.getConstructor(String.class);
            key = (Comparable) myConstructor.newInstance(clusteringKeyValue);

        } catch (Exception e) {
            e.printStackTrace();
        }
        int pageIndex = binarySearch(key) ;
        Page p = pages.get(pageIndex) ;
        Hashtable<Integer,Comparable> colNameVal = new Hashtable<Integer , Comparable>();
        for (Map.Entry e:colNameValue.entrySet()){
            int id = colNameId.get(e.getKey());
            Comparable val = (Comparable) e.getValue();
            colNameVal.put(id,val);
        }
        p.update( key , colNameVal)  ;
    }

    public void deleteTuple (Hashtable<String, Object> columnNameValue ){

        Hashtable<Integer,Comparable> colNameVal = new Hashtable<Integer , Comparable>();
        for (Map.Entry e:columnNameValue.entrySet()){
            int id = colNameId.get(e.getKey());
            Comparable val = (Comparable) e.getValue();
            colNameVal.put(id,val);
        }
        Vector<Page> newPages = new Vector<>();
        for (Page p : pages ){
            p.deleteTuples(colNameVal) ;
            if(!p.isEmpty() ) {
                newPages.add(p);
            }
        }
        pages = newPages;
    }
    public void add(Tuple row) throws IOException, ClassNotFoundException, DBAppException {
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
