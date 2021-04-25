import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Comparable<Tuple>, Serializable {
    int indexOfPK;
    Vector<Comparable> vector;

    public Comparable getPK() {
        return vector.get(indexOfPK);
    }
    public int compareTo(Tuple t) {
        return vector.get(indexOfPK).compareTo(t.vector.get(indexOfPK));
    }
}
