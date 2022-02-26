import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

/*
Get order count per customer for the month of 2014 January.
 * Tables - orders and customers
 * Data should be sorted in descending order by count and ascending order by customer id.
 * Output should contain customer_id, customer_first_name, customer_last_name and customer_order_count.
 */

public class UseCase1 {

    static final Logger logger = Logger.getLogger(UseCase1.class);
    public static String ORDER_CUSTOMER_ID= "order_customer_id";
    public static String ORDER_PATH = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";

    public static Dataset<Row> getOrders() {
        String ordersPath = ORDER_PATH;
        Dataset<Row> orders = util.getSparkSession().read().format("csv").option("header", true).option("inferSchema", true).load(ordersPath);
        return orders;
    }

    public static Dataset<Row> getCustomers() {
        String customersPath = ORDER_PATH;
        Dataset<Row> customers = util.getSparkSession().read().format("csv").option("header", true).option("inferSchema", true).load(customersPath);
        return customers;
    }

    public static Dataset<Row> getUseCase1Result(){
        Dataset<Row> orders = getOrders();
        Dataset<Row> customers = getCustomers();
     Dataset<Row> result = orders.join(customers, orders.col(ORDER_CUSTOMER_ID).equalTo(customers.col("customer_id"))).
                where(orders.col("order_date").like("2014-01%")).
                groupBy(customers.col("customer_id"),
                        customers.col("customer_fname"),
                        customers.col("customer_lname")).
                agg(count(orders.col("order_customer_id")).alias("customer_order_count")).
                orderBy(col("customer_order_count").desc(),customers.col("customer_id"));

     return result;
    }

    public static void main(String[] args){

        logger.info("------------------------------------------running UseCase 1------------------------------------------------------");

        getOrders().show();
        getOrders().printSchema();
        getCustomers().show();
        getCustomers().printSchema();
        getUseCase1Result().show();
        getUseCase1Result().printSchema();

        logger.info("------------------------------------------Write Result---------------------------------------------------------------");

        getUseCase1Result().coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase1");

        logger.info("--------------------------------------------Completed----------------------------------------------------------------");

    }
}
