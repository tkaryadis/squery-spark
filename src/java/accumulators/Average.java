package accumulators;

import java.io.Serializable;

public class Average implements Serializable {
    private long sum;
    private long count;

    // Constructors, getters, setters...
    public Average() {
    }

    public Average(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}