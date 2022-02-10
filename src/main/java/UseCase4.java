import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/*
Get the revenue generated for each category for the month of 2014 January
* Tables - orders, order_items, products and categories
* Data should be sorted in ascending order by category_id.
* Output should contain all the fields from category along with the revenue as category_revenue.
* Consider only COMPLETE and CLOSED orders
*/
public class UseCase4 {

    static final Logger logger = Logger.getLogger(UseCase4.class);

    public static Dataset<Row> getOrders() {
        String ordersPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = util.getSparkSession().read().format("csv").option("header", true).option("inferSchema", true).load(ordersPath);
        return orders;
    }

    public static Dataset<Row> getOrder_items() {
        String order_itemsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row>order_items=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(order_itemsPath);
        return order_items;
    }

    public static Dataset<Row> getProducts() {
        String productsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row>products=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        return products;
    }

    public static Dataset<Row> getCategories() {
        String categoriesPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row>categories=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        return categories;
    }

    public static Dataset<Row> getUseCase4Result() {
        Dataset<Row> orders = getOrders();
        Dataset<Row> order_items = getOrder_items();
        Dataset<Row> products = getProducts();
        Dataset<Row> categories =getCategories();
        Dataset<Row>join1=orders.join(order_items, orders.col("order_id").equalTo(order_items.col("order_item_order_id")));
        Dataset<Row>join2=products.join(join1, products.col("product_id").equalTo(join1.col("order_item_product_id")));
        Dataset<Row>result=join2.join(categories, join2.col("product_category_id").equalTo(categories.col("category_id"))).
                where(orders.col("order_date").like("2014-01%").and(join2.col("order_status").isin("COMPLETE","CLOSED"))).
                groupBy(categories.col("category_id"),
                        categories.col("category_department_id"),
                        categories.col("category_name")).
                agg(round(sum(join2.col("order_item_subtotal")),2).alias("category_revenue")).
                orderBy(categories.col("category_id"));
        return result;
    }

        public static void main(String[] args) {

            logger.info("------------------------------------------running UseCase 4------------------------------------------------------");

            getOrders().show();
            getOrders().printSchema();
            getOrder_items().show();
            getOrder_items().printSchema();
            getProducts().show();
            getProducts().printSchema();
            getCategories().show();
            getCategories().printSchema();
            getUseCase4Result().show();
            getUseCase4Result().printSchema();

            logger.info("------------------------------------------Write Result-----------------------------------------------------------");

            getUseCase4Result().coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase4");

            logger.info("--------------------------------------------Completed------------------------------------------------------------");


        }

}
