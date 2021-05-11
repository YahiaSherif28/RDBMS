import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Vector;

public class Page implements Serializable {
    private transient Vector<Tuple> data;
    private Comparable minPK;
    private String fileName;
    private Integer maxSize;
    private Integer curSize;

    public Page(String fileName) {
        data = new Vector();
        this.fileName = fileName;
        this.maxSize = DBApp.getMaximumRowsCountinTablePage();
        curSize = 0;
    }

    public Vector<Tuple> getData() {
        return data;
    }

    public void setData(Vector<Tuple> data) {
        this.data = data;
    }

    public Comparable getMinPK() {
        return minPK;
    }

    public void setMinPK(Comparable minPK) {
        this.minPK = minPK;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void loadPage() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(fileName));
        data = (Vector<Tuple>) oi.readObject();
        oi.close();
    }

    public void closePage() throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(fileName));
        minPK = data.get(0).getPK();
        curSize = data.size();
        os.writeObject(data);
        os.close();
        data = null;
    }

    public void add(Tuple row) throws IOException, ClassNotFoundException, DBAppException {
        loadPage();
        int index = Collections.binarySearch(data, row);
        if (index < 0) {
            index = -index - 1;
        } else {
            closePage();
            throw new DBAppException("Primary Key already exists");
        }
        data.add(index, row);
        closePage();
    }

    public Page split() throws IOException, ClassNotFoundException {
        loadPage();
        Page newPage = new Page(DBApp.getNextPageName());
        int mid = data.size() / 2;
        while (data.size() > mid) {
            newPage.getData().add(data.remove(data.size() - 1));
        }
        Collections.reverse(newPage.getData());
        closePage();
        newPage.closePage();
        return newPage;
    }

    public void update(Comparable key, Hashtable<Integer, Comparable> colNameVal) {

        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }


        int l = 0;
        int h = data.size() - 1;
        int id = -1;
        while (l <= h) {
            int mid = (l + h) / 2;
            if (data.get(mid).getPK().compareTo(key) <= 0) {
                if (data.get(mid).getPK().compareTo(key) == 0) id = mid;
                l = mid + 1;
            } else {
                h = mid - 1;
            }
        }

        if (id == -1) return;

        for (Map.Entry<Integer, Comparable> e : colNameVal.entrySet()) {
            data.get(id).getTupleData().set(e.getKey(), e.getValue());
        }


        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteTuples(Hashtable<Integer, Comparable> colNameVal) {
        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Vector<Tuple> newData = new Vector<>();
        for (Tuple t : data) {
            boolean f = true;
            for (Map.Entry<Integer, Comparable> e : colNameVal.entrySet()) {
                f &= t.getTupleData().get(e.getKey()).compareTo(e.getValue()) == 0;
            }
            if (!f) newData.add(t);
        }

        data = newData;
        try {
            closePage();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isFull() {
        return curSize.equals(maxSize);
    }

    public boolean isEmpty() {
        return curSize == 0;
    }

    public String toString() {
        try {
            loadPage();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fileName + " " + "\n");
        for (Tuple t : data) {
            sb.append(t.toString() + "\n");
        }
        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

}
