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
        if(this.length() == string.length())
            return this.s.compareTo(string.s);
        return this.length() - string.length();
    }

    public static void main(String[] args) {
        System.out.println(MyString.class.getName().toString());
    }
}
