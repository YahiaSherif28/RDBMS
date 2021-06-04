import java.io.*;
import java.util.Vector;

public class Bucket implements Serializable {
    private String bucketName;
    private Bucket nextBucket;
    private Integer maxSize;
    private Integer curSize;
    transient private Vector<BucketPair> pages;

    public Bucket() {
        try {
            bucketName = DBApp.getNextBucketName();
            maxSize = DBApp.getMaximumKeysCountinIndexBucket();
            curSize = 0;
            nextBucket = null;
            pages = new Vector<BucketPair>();
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Vector<BucketPair> getBuckets() throws IOException, ClassNotFoundException {
        loadBucket();
        Vector<BucketPair> res = this.pages;
        closeBucket();
        return res;
    }

    private void loadBucket() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(bucketName));
        pages = (Vector<BucketPair>) oi.readObject();
        oi.close();
    }

    private void closeBucket() throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(bucketName));
        curSize = pages.size();
        os.writeObject(pages);
        os.close();
        pages = null;
    }


    public void add(BucketPair insertedTuple) {
        if (curSize.equals(maxSize)) {
            if (nextBucket == null)
                nextBucket = new Bucket();      //needed: increament & decreament curSize
            nextBucket.add(insertedTuple);
        }

        try {
            loadBucket();
            pages.add(insertedTuple);
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void delete(BucketPair deletedTuple) {
        try {
            loadBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (pages.contains(deletedTuple))
            pages.remove(deletedTuple);
        else
            nextBucket.delete(deletedTuple);

        try {
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updatePage(BucketPair updatedTuple, String newPageName) {
        try {
            loadBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (pages.contains(updatedTuple)) {
            pages.get(pages.indexOf(updatedTuple)).setPage(newPageName);
        } else
            nextBucket.updatePage(updatedTuple, newPageName);

        try {
            closeBucket();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}