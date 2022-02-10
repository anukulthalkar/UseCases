import org.apache.log4j.Logger;
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

    static final Logger logger = Logger.getLogger(UseCase2.class);

    public static Dataset<Row> getOrders() {
        String ordersPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = util.getSparkSession().read().format("csv").option("header", true).option("inferSchema", true).load(ordersPath);
        return orders;
    }

    public static Dataset<Row> getCustomers() {
        String customersPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = util.getSparkSession().read().format("csv").option("header", true).option("inferSchema", true).load(customersPath);
        return customers;
    }

    public static Dataset<Row> getUseCase2Result() {
        Dataset<Row> orders = getOrders();
        Dataset<Row> customers = getCustomers();
        Dataset<Row> result = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")), "right_outer").
                where(orders.col("order_date").like("2014-01%").and(orders.col("order_id").isNull())).
                select(customers.col("customer_id"),
                        customers.col("customer_fname"),
                        customers.col("customer_lname"),
                        customers.col("customer_email"),
                        customers.col("customer_password"),
                        customers.col("customer_street"),
                        customers.col("customer_city"),
                        customers.col("customer_state"),
                        customers.col("customer_zipcode")).
                orderBy(customers.col("customer_id"));
        return result;
    }

    public static long getOrdersCount(){
        long resultCount = getOrders().count();
        return resultCount;

    }

    public static long getCustomersCount() {
        long customersCount = getCustomers().count();
        return customersCount;
    }

    public static long getUseCase2ResultCount() {
        long resultCount = getUseCase2Result().count();
        return resultCount;
    }

    public static void main(String[] args){
        logger.info("------------------------------------------running UseCase 2------------------------------------------------------");

        UseCase1.getOrders().show();
        UseCase1.getOrders().printSchema();
        UseCase1.getCustomers().show();
        UseCase1.getCustomers().printSchema();
        getUseCase2Result().show();
        System.out.println(getUseCase2ResultCount());
        getUseCase2Result().printSchema();

        logger.info("------------------------------------------Write Result--------------------------------------------------");

        getUseCase2Result().coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase2");

        logger.info("--------------------------------------------Completed---------------------------------------------------");

    }
}
