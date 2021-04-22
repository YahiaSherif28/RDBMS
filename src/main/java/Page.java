import java.io.*;
import java.util.Collections;
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

    public void add(Tuple row) throws IOException, ClassNotFoundException {
        loadPage();
        int index = Collections.binarySearch(data, row);
        if (index < 0) {
            index = -index - 1;
        }
        data.add(index, row);
        closePage();
        curSize++;
    }

    public Page split() throws IOException, ClassNotFoundException {
        loadPage();
        Page newPage = new Page(DBApp.getNextPageName());
        int mid = data.size() / 2;
        while (data.size() > mid) {
            newPage.getData().add(data.remove(data.size() - 1));
        }
        closePage();
        newPage.closePage();
        return newPage;
    }

    public boolean isFull() {
        return curSize == maxSize;
    }

    public String toString() {
        try {
            loadPage();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String s = fileName + "\n" + data.toString();
        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return s;
    }
}
