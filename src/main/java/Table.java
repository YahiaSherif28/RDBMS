import java.io.IOException;
import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    private static Integer maxSize;
    private Vector<Page> pages;

    public void add(Tuple row) throws IOException, ClassNotFoundException {
        if(pages.isEmpty()) {
            Page newPage = new Page(DBApp.getNextPageName(), maxSize);
            newPage.getData().add(row);
            newPage.closePage();
            return;
        }
        int insertionIndex = binarySearch(row.getPK());
        if (pages.get(insertionIndex).isFull()) {
            pages.get(insertionIndex).add(row);
            Page newPage = pages.get(insertionIndex).split();
            pages.add(insertionIndex + 1, newPage);
        } else {
            pages.get(insertionIndex).add(row);
        }
    }

    private int binarySearch(Comparable key) {
        int lo = 0;
        int hi = pages.size() - 1;
        int ans = pages.size();
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (pages.get(mid).getMinPK().compareTo(key) > 0) {
                ans = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return Math.max(ans - 1, 0);
    }
}
