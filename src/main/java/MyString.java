import java.io.Serializable;

public class MyString implements Comparable<MyString>, Serializable {
    String s;

    public MyString(String s) {
        this.s = s;
    }

    public int length() {
        return s.length();
    }

    public int compareTo(MyString string) {
            return this.s.compareTo(string.s);
    }
    public String toString() {
        return s;
    }
}
