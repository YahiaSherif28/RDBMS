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
    private static Integer maxSize;
    private Vector<Page> pages;

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

        colTypes = new Vector<>();
        colMin = new Vector<>();
        colMax = new Vector<>();
        for(String name : colNames){
            colTypes.add(colNameType.get(name));
            String strColType =colNameType.get(name);
            Class myClass = Class.forName(strColType);
            Constructor myConstructor = myClass.getConstructor();
            Comparable myMin = (Comparable) myConstructor.newInstance(colNameMin.get(name));
            Comparable myMax = (Comparable) myConstructor.newInstance(colNameMax.get(name));
            colMin.add(myMin);
            colMax.add(myMax);
        }
    }

    public void add(Tuple row) throws IOException, ClassNotFoundException {
        if(pages.isEmpty()) {
            Page newPage = new Page(DBApp.getNextPageName(), maxSize);
            newPage.getData().add(row);
            newPage.closePage();
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

}
