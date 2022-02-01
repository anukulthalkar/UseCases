import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

/*
Get the revenue generated for each category for the month of 2014 January
* Tables - orders, order_items, products and categories
* Data should be sorted in ascending order by category_id.
* Output should contain all the fields from category along with the revenue as category_revenue.
* Consider only COMPLETE and CLOSED orders
*/
public class UseCase4 {
    public static void main(String[] args) {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();
            String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
            String order_itemsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
            String productsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
            String categoriesPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
            Dataset<Row> orders=spark.read().format("csv").option("header",true).option("inferschema",true).load(ordersPath);
            Dataset<Row>order_items=spark.read().format("csv").option("header",true).option("inferschema",true).load(order_itemsPath);
            Dataset<Row>products=spark.read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
            Dataset<Row>categories=spark.read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
            orders.show();
            order_items.show();
            products.show();
            categories.show();
            Dataset<Row>join1=orders.join(order_items, orders.col("order_id").equalTo(order_items.col("order_item_order_id")));
            join1.show();
            Dataset<Row>join2=products.join(join1, products.col("product_id").equalTo(join1.col("order_item_product_id")));
            join2.show();
            Dataset<Row>result=join2.join(categories, join2.col("product_category_id").equalTo(categories.col("category_id"))).
                    where(orders.col("order_date").like("2014-01%").and(join2.col("order_status").isin("COMPLETE","CLOSED"))).
                    groupBy(categories.col("category_id"),
                            categories.col("category_department_id"),
                            categories.col("category_name")).
                    agg(round(sum(join2.col("order_item_subtotal")),2).alias("category_revenue")).
                    orderBy(categories.col("category_id"));

            result.show(100);

    }

}
