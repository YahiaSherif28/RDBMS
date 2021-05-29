
public class Range {
    Comparable min, max;
    boolean isMinInclusive, isMaxInclusive;

    public Range(Comparable min, Comparable max, boolean isMinInclusive, boolean isMaxInclusive) {
        this.min = min;
        this.max = max;
        this.isMinInclusive = isMinInclusive;
        this.isMaxInclusive = isMaxInclusive;
    }

    public Range(Comparable min, Comparable max) {
        this.min = min;
        this.max = max;
        isMinInclusive = true;
        isMaxInclusive = true;
    }

    static final String LT = "<", LTE = "<=", GT = ">", GTE = ">=", E = "=", NE = "!=";

    public Range intersect(SQLTerm sqlTerm) {
        if (sqlTerm._strOperator.equals(LT)) {
            if (this.max.compareTo(sqlTerm._objValue) >= 0) {
                max = (Comparable) sqlTerm._objValue;
                isMaxInclusive = false;
            }
        } else if (sqlTerm._strOperator.equals(LTE)) {
            if (this.max.compareTo(sqlTerm._objValue) > 0) {
                max = (Comparable) sqlTerm._objValue;
                isMaxInclusive = true;
            }
        } else if (sqlTerm._strOperator.equals(GT)) {
            if (this.min.compareTo(sqlTerm._objValue) <= 0) {
                min = (Comparable) sqlTerm._objValue;
                isMinInclusive = false;
            }
        } else if (sqlTerm._strOperator.equals(GTE)) {
            if (this.min.compareTo(sqlTerm._objValue) < 0) {
                min = (Comparable) sqlTerm._objValue;
                isMinInclusive = true;
            }
        } else if (sqlTerm._strOperator.equals(E)) {
            if (this.max.compareTo(sqlTerm._objValue) > 0) {
                max = (Comparable) sqlTerm._objValue;
                isMaxInclusive = true;
            }
            if (this.min.compareTo(sqlTerm._objValue) < 0) {
                min = (Comparable) sqlTerm._objValue;
                isMinInclusive = true;
            }
        }
        return this;
    }

    public boolean isDegenerate() {
        return min.compareTo(max) > 0 || (min.compareTo(max) == 0 && !(isMaxInclusive && isMinInclusive));
    }

    // add any methods you need here, I will add some for my part also
}
