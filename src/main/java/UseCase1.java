
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/*
Get order count per customer for the month of 2014 January.
 * Tables - orders and customers
 * Data should be sorted in descending order by count and ascending order by customer id.
 * Output should contain customer_id, customer_first_name, customer_last_name and customer_order_count.
 */

public class UseCase1 {
    public static void main(String[] args){

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> orders = spark.read().format("csv").option("header",true).option("inferSchema",true).load(ordersPath);
        Dataset<Row> customers =spark.read().format("csv").option("header",true).option("inferSchema",true).load(customersPath);
        Dataset<Row> result = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id"))).
                where(orders.col("order_date").like("2014-01%")).
                groupBy(customers.col("customer_id"),
                        customers.col("customer_fname"),
                        customers.col("customer_lname")).
                agg(count(orders.col("order_customer_id")).alias("customer_order_count")).
                orderBy(customers.col("customer_id"),col("customer_order_count").desc());
        //orders.show();
        //customers.show();
        result.show();
        //orders.printSchema();
        //customers.printSchema();



         }
}
