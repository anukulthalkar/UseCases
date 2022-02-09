import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

public class UseCase3Test {
    @Test
    public void validateOrders() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header",true).option("inferSchema",true).load(ordersPath);
        long count=orders.count();
        Assert.assertTrue(UseCase3.validateOrders(count));
    }
    @Test
    public void validateCustomers(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers =spark.read().format("csv").option("header",true).option("inferSchema",true).load(customersPath);
        long count=customers.count();
        Assert.assertTrue(UseCase3.validateCustomers(count));
    }

    @Test
    public void validateOrder_items(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String order_itemsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row>order_items=spark.read().format("csv").option("header",true).option("inferschema",true).load(order_itemsPath);
        long count = order_items.count();
        Assert.assertTrue(UseCase3.validateOrder_items(count));
    }

    @Test
    public void validateResult(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        String order_itemsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row>orders=spark.read().format("csv").option("header",true).option("inferschema",true).load(ordersPath);
        Dataset<Row>customers=spark.read().format("csv").option("header",true).option("inferschema",true).load(customersPath);
        Dataset<Row>order_items=spark.read().format("csv").option("header",true).option("inferschema",true).load(order_itemsPath);
        Dataset<Row>join1=orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")),"right_outer");
        Dataset<Row>result=join1.join(order_items, join1.col("order_id").equalTo(order_items.col("order_item_order_id"))).
                where(orders.col("order_date").like("2014-01%").and(join1.col("order_status").isin("COMPLETE","CLOSED"))).
                groupBy(join1.col("customer_id"),
                        join1.col("customer_fname"),
                        join1.col("customer_lname")).
                agg(coalesce(round(sum(order_items.col("order_item_subtotal")),2),lit(0)).alias("customer_revenue")).
                orderBy(col("customer_revenue").desc(),join1.col("customer_id"));
        long count = result.count();
        Assert.assertTrue(UseCase3.validateResult(count));
    }

}
