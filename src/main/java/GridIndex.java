import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex implements Serializable {
    private Integer dimension;
    private String[] columns;
    private Comparable[][] minRange;
    private Comparable[][] maxRange;
    private Object[] grid;

    public GridIndex(String[] columns, Comparable[][] minRange, Comparable[][] maxRange) {
        this.columns = columns;
        this.minRange = minRange;
        this.maxRange = maxRange;
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
                for(int j = 0; j < 10; j++)
                    if(colVal.compareTo(minRange[i][j]) >= 0 && colVal.compareTo(maxRange[i][j]) <= 0) {
                        rangeIndices.add(j + 1);
                        break;
                    }
            }
        }
        return rangeIndices;
    }

    public void insertTuple(Vector<Comparable> values, String pageName) {
        Vector<Integer> rangeIndices = getRangeIndicesFromValues(values);
        insertInGridRec(grid, rangeIndices, 1, dimension, pageName);
    }

    private void insertInGridRec(Object grid, Vector<Integer> rangeIndices, int curDimension, int maxDimension, String pageName) {
        Object[] array = (Object[]) grid;
        Object nextObject = array[rangeIndices.get(curDimension - 1)];
        if(curDimension == maxDimension) {
            if(nextObject == null)
                nextObject = array[rangeIndices.get(curDimension - 1)] = new Bucket();
            ((Bucket)nextObject).add(pageName);
            return;
        }
        insertInGridRec(nextObject, rangeIndices, curDimension + 1, maxDimension, pageName);
    }

    public void deleteTuple(Vector<Comparable> values, String pageName) {
        Vector<Integer> indices = getRangeIndicesFromValues(values);
        deleteFromGridRec(grid, indices, 1, dimension, pageName);
    }

    private void deleteFromGridRec(Object grid, Vector<Integer> rangeIndices, int curDimension, int maxDimension, String pageName) {
        Object[] array = (Object[]) grid;
        Object nextObject = array[rangeIndices.get(curDimension - 1)];
        if(curDimension == maxDimension) {
            if(nextObject == null)
                nextObject = array[rangeIndices.get(curDimension - 1)] = new Bucket();
            ((Bucket)nextObject).delete(pageName);
            return;
        }
        deleteFromGridRec(nextObject, rangeIndices, curDimension + 1, maxDimension, pageName);
    }

    public boolean equals(String[] columns) {
        if(this.columns.length != columns.length)
            return false;
        for(int i = 0; i < columns.length; i++)
            if(!this.columns[i].equals(columns[i]))
                return false;
        return true;
    }

    // returns the names of pages to be opened
    // Use hashtable just as a hashset. HashSet is not thread-safe.
    // take a look at the Range class
    // ranges are in the same order as the columns in the index
    public Hashtable<String,Integer> select (Range[] ranges) throws DBAppException {
        if(ranges.length != dimension) {
            throw new DBAppException();
        }
        return null;
    }

}