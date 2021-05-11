import javax.management.ValueExp;
import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Comparable<Tuple>, Serializable {
    private int indexOfPK;
    private Vector<Comparable> vector;

    public Tuple(Vector<Comparable> vector, int indexOfPK) {
        this.vector = vector;
        this.indexOfPK = indexOfPK;
    }

    public Comparable getPK() {
        return vector.get(indexOfPK);
    }

    public Vector<Comparable> getTupleData() {
        return vector;
    }

    public int compareTo(Tuple t) {
        return vector.get(indexOfPK).compareTo(t.vector.get(indexOfPK));
    }

    public String toString() {
        return vector.toString();
    }

}