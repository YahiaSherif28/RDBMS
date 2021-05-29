import java.util.*;
import java.io.*;

public class BucketPair implements Serializable {
    private Vector<Comparable> value;
    private String page;

    public BucketPair(Vector<Comparable> value, String page) {
        this.value = value;
        this.page = page;
    }

    public boolean equals(Object o) {
        BucketPair b = (BucketPair) o;
        return this.value.equals(b.value) && this.page.equals(b.page);
    }
    public String getPage () {
        return page ;
    }
}
