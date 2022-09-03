package accumulators;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

//arg to product, product, return final type
public class ProductAcc extends Aggregator<Long, Product, Long> {
    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    public Product zero() {
        return new Product(1);
    }
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object
    public Product reduce(Product buffer, Long data)
    {
        buffer.setProduct(buffer.getProduct() * data);
        return buffer;
    }
    // Merge two intermediate values
    public Product merge(Product b1, Product b2) {
        b1.setProduct(b1.getProduct() * b2.getProduct());
        return b1;
    }
    // Transform the output of the reduction
    public Long finish(Product reduction) {
        return reduction.getProduct();
    }
    // Specifies the Encoder for the intermediate value type
    public Encoder<Product> bufferEncoder() {
        return Encoders.bean(Product.class);
    }
    // Specifies the Encoder for the final output value type, here returns a long
    public Encoder<Long> outputEncoder() {
        return Encoders.LONG();
    }
}