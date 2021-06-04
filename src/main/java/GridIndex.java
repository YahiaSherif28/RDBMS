import javax.xml.stream.events.Comment;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class GridIndex implements Serializable {
    private Integer dimension;
    private String[] columns;
    private Comparable[][] minRange;
    private Object[] grid;

    public GridIndex(String[] columns, Comparable[][] minRange) {
        this.columns = columns;
        this.minRange = minRange;
        this.dimension = columns.length;

        grid = new Object[11];
        createGridRec(grid, 1, dimension);
    }

    private void createGridRec(Object[] grid, int curDimension, int maxDimension) {
        if(curDimension == maxDimension)
            return;
        for(int i = 0; i < 11; i++) {
            grid[i] = new Object[11];
            createGridRec((Object[])grid[i], curDimension + 1, maxDimension);
        }
    }

    private Vector<Integer> getRangeIndicesFromValues(Vector<Comparable> values) {
        Vector<Integer> rangeIndices = new Vector<>();
        for(int i = 0; i < dimension; i++) {
            Comparable colVal = values.get(i);
            if(colVal == null)
                rangeIndices.add(0);
            else {
                if(colVal instanceof MyString)
                    colVal = ((MyString) colVal).hashValue();
                boolean inserted = false;
                for(int j = 1; j < 10; j++)
                    if(colVal.compareTo(minRange[i][j]) < 0) {
                        rangeIndices.add(j);
                        inserted = true;
                        break;
                    }
                if(!inserted)
                    rangeIndices.add(10);
            }
        }
        return rangeIndices;
    }

    public void insertTuple(Vector<Comparable> values, String pageName) {
        Vector<Integer> rangeIndices = getRangeIndicesFromValues(values);
        insertInGridRec(grid, rangeIndices, values, 1, dimension, pageName);
    }

    private void insertInGridRec(Object grid, Vector<Integer> rangeIndices, Vector<Comparable> values, int curDimension, int maxDimension, String pageName) {
        Object[] array = (Object[]) grid;
        Object nextObject = array[rangeIndices.get(curDimension - 1)];
        if(curDimension == maxDimension) {
            if(nextObject == null)
                nextObject = array[rangeIndices.get(curDimension - 1)] = new Bucket();
            BucketPair insertedTuple = new BucketPair(values, pageName);
            ((Bucket)nextObject).add(insertedTuple);
            return;
        }
        insertInGridRec(nextObject, rangeIndices, values, curDimension + 1, maxDimension, pageName);
    }

    public void deleteTuple(Vector<Comparable> values, String pageName) {
        Vector<Integer> indices = getRangeIndicesFromValues(values);
        deleteFromGridRec(grid, indices, values, 1, dimension, pageName);
    }

    private void deleteFromGridRec(Object grid, Vector<Integer> rangeIndices, Vector<Comparable> values, int curDimension, int maxDimension, String pageName) {
        Object[] array = (Object[]) grid;
        Object nextObject = array[rangeIndices.get(curDimension - 1)];
        if(curDimension == maxDimension) {
            if(nextObject == null)
                nextObject = array[rangeIndices.get(curDimension - 1)] = new Bucket();
            BucketPair deletedTuple = new BucketPair(values, pageName);
            ((Bucket)nextObject).delete(deletedTuple);
            return;
        }
        deleteFromGridRec(nextObject, rangeIndices, values, curDimension + 1, maxDimension, pageName);
    }

    public void updateTuplePage(Vector<Comparable> values, String oldPageName, String newPageName) {
        Vector<Integer> indices = getRangeIndicesFromValues(values);
        updatePageInGridRec(grid, indices, values, 1, dimension, oldPageName, newPageName);
    }

    private void updatePageInGridRec(Object grid, Vector<Integer> rangeIndices, Vector<Comparable> values, int curDimension, int maxDimension, String oldPageName, String newPageName) {
        Object[] array = (Object[]) grid;
        Object nextObject = array[rangeIndices.get(curDimension - 1)];
        if(curDimension == maxDimension) {
            if(nextObject == null)
                nextObject = array[rangeIndices.get(curDimension - 1)] = new Bucket();
            BucketPair deletedTuple = new BucketPair(values, oldPageName);
            ((Bucket)nextObject).updatePage(deletedTuple, newPageName);
            return;
        }
        updatePageInGridRec(nextObject, rangeIndices, values, curDimension + 1, maxDimension, oldPageName, newPageName);
    }

    public boolean equals(String[] columns) {
        if(this.columns.length != columns.length)
            return false;
        for(int i = 0; i < columns.length; i++)
            if(!this.columns[i].equals(columns[i]))
                return false;
        return true;
    }

    public String[] getColumns() {
        return columns;
    }

    public HashSet<String> select(Hashtable<String,Range> colNameToRange) throws IOException, ClassNotFoundException {

        Vector<Comparable> start = new Vector<>();
        for(String s: columns){
            start.add(colNameToRange.get(s).min);
        }

        Vector<Comparable> end = new Vector<>();
        for(String s: columns){
            end.add(colNameToRange.get(s).max);
        }

        Vector<Integer> startIndicies = getRangeIndicesFromValues(start);
        Vector<Integer> endIndicies = getRangeIndicesFromValues(end);
        return getAllPagesNames(grid,startIndicies,endIndicies,1,dimension);
    }

    private HashSet<String> getAllPagesNames(Object grid, Vector<Integer> startIndicies , Vector<Integer> endIndicies, int curDimension, int maxDimension) throws IOException, ClassNotFoundException {
        Object[] array = (Object[]) grid;
        HashSet<String> retAll = new HashSet<>();
        for(int i = startIndicies.get(curDimension-1); i<=endIndicies.get(curDimension-1); i++){
        Object nextObject = array[i];
        if(curDimension == maxDimension) {
            if(nextObject == null)
               return new HashSet<>();
            HashSet<String> ret = new HashSet<>();
            for (BucketPair p : ((Bucket)nextObject).getBuckets()){
                ret.add(p.getPage());
            }
            return ret;
        }
            retAll.addAll(getAllPagesNames(nextObject, startIndicies,endIndicies, curDimension + 1, maxDimension));
        }
        return retAll;
    }

    // returns the names of pages to be open
    // take a look at the Range class
    // ranges are in the same order as the columns in the index
    

}
