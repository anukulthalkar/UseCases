

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/*
Get the customer details who have not placed any order for the month of 2014 January.
 * Tables - orders and customers
 * Data should be sorted in ascending order by customer_id
 * Output should contain all the fields from customers
 */

public class UseCase2 {
    public static void main(String[] args){

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header",true).option("inferSchema",true).load(ordersPath);
        Dataset<Row> customers =spark.read().format("csv").option("header",true).option("inferSchema",true).load(customersPath);
        Dataset<Row> result = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")),"right_outer").
                where(orders.col("order_date").like("2014-01%").and(orders.col("order_id").isNull())).
                select(customers.col("*")).
                orderBy(customers.col("customer_id"));
        orders.show();
        customers.show();
        result.show();
        result.coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase2");

    }
}
