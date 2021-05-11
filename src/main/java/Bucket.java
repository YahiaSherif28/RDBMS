import java.io.*;
import java.util.Vector;

public class Bucket implements Serializable {
    private String bucketName;
    private Bucket nextBucket;
    private Integer maxSize;
    private Integer curSize;
    transient private Vector<String> pages;

    public Bucket() {
        try {
            bucketName = DBApp.getNextBucketName();
            maxSize = DBApp.getMaximumKeysCountinIndexBucket();
            curSize = 0;
            nextBucket = null;
            pages = new Vector<String>();
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void loadBucket() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(bucketName));
        pages = (Vector<String>) oi.readObject();
        oi.close();
    }

    public void closeBucket() throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(bucketName));
        curSize = pages.size();
        os.writeObject(pages);
        os.close();
        pages = null;
    }

    public void add(String pageName) {
        if(curSize.equals(maxSize)) {
            if (nextBucket == null)
                nextBucket = new Bucket();
            nextBucket.add(pageName);
        }

        try {
            loadBucket();
            pages.add(pageName);
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(String pageName) {
        try {
            loadBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(pages.contains(pageName))
            pages.remove(pageName);
        else
            nextBucket.delete(pageName);

        try {
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
