import org.apache.log4j.Logger;
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
    static long a = 0;
    static final Logger logger = Logger.getLogger(UseCase1.class);
    public static void main(String args){
        logger.info("------------------------------------------running usecase 1------------------------------------------------------");

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("------------------------------------------spark session created--------------------------------------------------");

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
                orderBy(col("customer_order_count").desc(),customers.col("customer_id"));
        //orders.show();
        //customers.show();
        a = result.count();
        result.show();
        System.out.println(a);
        //orders.printSchema();
        //customers.printSchema();
        result.coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase1");

         }
}
