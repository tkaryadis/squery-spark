package accumulators;

import java.io.Serializable;

public class Product implements Serializable {
    private long product;

    // Constructors, getters, setters...
    public Product() {
        this.product=1;
    }

    public Product(long product) {
        this.product = product;
    }

    public long getProduct() {
        return product;
    }

    public void setProduct(long product) {
        this.product = product;
    }
}