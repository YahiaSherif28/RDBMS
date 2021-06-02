import java.io.Serializable;

public class MyString implements Comparable<MyString>, Serializable {
    String s;

    public MyString(String s) {
        this.s = s;
    }

    public int length() {
        return s.length();
    }

    public long hashValue() {
        long p = 257;
        long ret = 0;
        for (int i = 6; i >= 0; i--) {
            int val = i < s.length() ? (int) s.charAt(i) : 0;
            ret *= p;
            ret += val;
        }
        return ret;
    }

    public int compareTo(MyString string) {
        return this.s.compareTo(string.s);
    }

    public String toString() {
        return s;
    }
}
