import java.io.*;
import java.sql.Array;
import java.util.*;

public class Page implements Serializable {
    private transient Vector<Tuple> data;
    private Comparable minPK;
    private String fileName;
    private Integer maxSize;
    private Integer curSize;

    public Page(String fileName) {
        data = new Vector();
        this.fileName = fileName;
        this.maxSize = DBApp.getMaximumRowsCountinTablePage();
        curSize = 0;
    }

    public Vector<Tuple> loadAndGetData() {
        Vector<Tuple> temp = null;
        try {
            loadPage();
            temp = data;
            closePage();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return temp;
    }

    public Vector<Tuple> getData() {
        return data;
    }

    public void setData(Vector<Tuple> data) {
        this.data = data;
    }

    public Comparable getMinPK() {
        return minPK;
    }

    public void setMinPK(Comparable minPK) {
        this.minPK = minPK;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void loadPage() throws IOException, ClassNotFoundException {
        ObjectInputStream oi = new ObjectInputStream(new FileInputStream(fileName));
        data = (Vector<Tuple>) oi.readObject();
        oi.close();
    }

    public void closePage() throws IOException {
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(fileName));
        minPK = data.get(0).getPK();
        curSize = data.size();
        os.writeObject(data);
        os.close();
        data = null;
    }

    public void add(Tuple row) throws IOException, ClassNotFoundException, DBAppException {
        loadPage();
        int index = Collections.binarySearch(data, row);
        if (index < 0) {
            index = -index - 1;
        } else {
            closePage();
            throw new DBAppException("Primary Key already exists");
        }
        data.add(index, row);
        closePage();
    }

    public Page split() throws IOException, ClassNotFoundException {
        loadPage();
        Page newPage = new Page(DBApp.getNextPageName());
        int mid = data.size() / 2;
        while (data.size() > mid) {
            newPage.getData().add(data.remove(data.size() - 1));
        }
        Collections.reverse(newPage.getData());
        closePage();
        newPage.closePage();
        return newPage;
    }

    public void update(Comparable key, Hashtable<Integer, Comparable> colNameVal) {



        int id = searchForTuple(key) ;

        if (id == -1) return;

        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Map.Entry<Integer, Comparable> e : colNameVal.entrySet()) {
            data.get(id).getTupleData().set(e.getKey(), e.getValue());
        }


        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Tuple getTuple (Comparable key) {
        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Tuple ret = data.get(searchForTuple(key));
        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret ;



    }
    public int searchForTuple (Comparable key) {
        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int l = 0;
        int h = data.size() - 1;
        int id = -1;
        while (l <= h) {
            int mid = (l + h) / 2;
            if (data.get(mid).getPK().compareTo(key) <= 0) {
                if (data.get(mid).getPK().compareTo(key) == 0) id = mid;
                l = mid + 1;
            } else {
                h = mid - 1;
            }
        }

        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return id ;
    }

    public Vector<Tuple> deleteTuples(Hashtable<Integer, Comparable> colNameVal) {
        try {
            loadPage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Vector<Tuple> newData = new Vector<>();
        Vector<Tuple> deletedTuples = new Vector<Tuple>() ;
        for (Tuple t : data) {
            boolean f = true;
            for (Map.Entry<Integer, Comparable> e : colNameVal.entrySet()) {
                f &= t.getTupleData().get(e.getKey()).compareTo(e.getValue()) == 0;
            }
            if (!f) newData.add(t);
            else deletedTuples.add(t) ;
        }

        data = newData;
        try {
            closePage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deletedTuples ;
    }

    public boolean isFull() {
        return curSize.equals(maxSize);
    }

    public boolean isEmpty() {
        return curSize == 0;
    }

    public String toString() {
        try {
            loadPage();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fileName + " " + "\n");
        for (Tuple t : data) {
            sb.append(t.toString() + "\n");
        }
        try {
            closePage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public Vector<Tuple> select(SQLTerm[] sqlTerms, String[] arrayOperators , TreeMap<String,Integer> colNameId) throws DBAppException, IOException {
        try {
            loadPage();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Vector<Tuple> ret = new Vector<>();
        for (Tuple t : this.data){
            if(seprateOr(sqlTerms,arrayOperators,0,sqlTerms.length-1 , t , colNameId)) ret.add(t);
        }
        closePage();
        return ret;
    }

    public boolean seprateOr(SQLTerm[] sqlTerms, String[] arrayOperators , int start , int end,Tuple t , TreeMap<String,Integer> colNameId) throws DBAppException {
        for (int i =start; i<end; i++ ){
            if(arrayOperators[i].equals("OR")){
                return seprateXor(sqlTerms,arrayOperators,start,i,t ,colNameId)||seprateOr(sqlTerms,arrayOperators,i+1,end,t , colNameId);
            }
        }
        return seprateXor(sqlTerms,arrayOperators,start,end,t,colNameId);
    }

    public boolean seprateAnd(SQLTerm[] sqlTerms, String[] arrayOperators , int start , int end,Tuple t , TreeMap<String,Integer> colNameId) throws DBAppException {
        for (int i =start; i<end; i++ ){
            if(arrayOperators[i].equals("AND")){
                return checkTerm(sqlTerms,arrayOperators,start,i,t ,colNameId)&&seprateAnd(sqlTerms,arrayOperators,i+1,end,t , colNameId);
            }
        }
        return checkTerm(sqlTerms,arrayOperators,start,end,t,colNameId);
    }

    public boolean seprateXor(SQLTerm[] sqlTerms, String[] arrayOperators , int start , int end,Tuple t , TreeMap<String,Integer> colNameId) throws DBAppException {
        for (int i =start; i<end; i++ ){
            if(arrayOperators[i].equals("XOR")){
                return seprateAnd(sqlTerms,arrayOperators,start,i,t ,colNameId)^seprateXor(sqlTerms,arrayOperators,i+1,end,t , colNameId);
            }
        }
        return seprateAnd(sqlTerms,arrayOperators,start,end,t,colNameId);
    }

    public boolean checkTerm(SQLTerm[] sqlTerms, String[] arrayOperators , int start , int end,Tuple t , TreeMap<String,Integer> colNameId) throws DBAppException {
        if(start!=end ){
            throw new DBAppException("ERROR IN SEPRATING");
        }
        return evaluate(sqlTerms[start] , t , colNameId );
    }

    public boolean evaluate(SQLTerm sqlTerm , Tuple t , TreeMap<String,Integer> colNameId) throws DBAppException {
        if(sqlTerm._strOperator.equals("=")) return equal((Comparable) sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        if(sqlTerm._strOperator.equals("<")) return lessThan((Comparable)sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        if(sqlTerm._strOperator.equals(">")) return moreThan((Comparable)sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        if(sqlTerm._strOperator.equals("<=")) return lessThanOrEqual((Comparable)sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        if(sqlTerm._strOperator.equals(">=")) return moreThanOrEqual((Comparable)sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        if(sqlTerm._strOperator.equals("!=")) return notEqual((Comparable)sqlTerm._objValue,t.getTupleData().get(colNameId.get(sqlTerm._strColumnName)));
        throw (new DBAppException("Invalid strOperator"));
    }

    public  boolean equal( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)==0;
    }
    public  boolean lessThan( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)<0;
    }
    public  boolean moreThan( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)>0;
    }
    public  boolean lessThanOrEqual( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)<=0;
    }
    public  boolean moreThanOrEqual( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)>=0;
    }
    public  boolean notEqual( Comparable val , Comparable tupleVal){
        return tupleVal.compareTo(val)!=0;
    }

}