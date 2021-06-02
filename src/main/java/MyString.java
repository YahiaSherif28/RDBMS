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

    long p = 257;

    public long hashValue (int index){
        long ret =0;
        for(int i = index ; i<index+5; i++){
            int val = i<s.length() ?(int)s.charAt(i) :0;

            ret *= p;
            ret+=val;

        }
        return ret;
    }
}
