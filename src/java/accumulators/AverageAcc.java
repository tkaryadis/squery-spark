package accumulators;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class AverageAcc extends Aggregator<Long, Average, Double> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    public Average zero() {
        return new Average(0L, 0L);
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    public Average reduce(Average buffer, Long data) {
        long newSum = buffer.getSum() + data;
        long newCount = buffer.getCount() + 1;
        buffer.setSum(newSum);
        buffer.setCount(newCount);
        return buffer;
    }
    // Merge two intermediate values
    public Average merge(Average b1, Average b2) {
        long mergedSum = b1.getSum() + b2.getSum();
        long mergedCount = b1.getCount() + b2.getCount();
        b1.setSum(mergedSum);
        b1.setCount(mergedCount);
        return b1;
    }
    // Transform the output of the reduction
    public Double finish(Average reduction) {
        return ((double) reduction.getSum()) / reduction.getCount();
    }
    // Specifies the Encoder for the intermediate value type
    public Encoder<Average> bufferEncoder() {
        return Encoders.bean(Average.class);
    }
    // Specifies the Encoder for the final output value type
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}