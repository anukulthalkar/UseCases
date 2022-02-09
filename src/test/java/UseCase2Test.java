import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

    public class UseCase2Test {
        @Test
        public void validateOrders() {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();
            String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
            Dataset<Row> orders = spark.read().format("csv").option("header",true).option("inferSchema",true).load(ordersPath);
            long count=orders.count();
            Assert.assertTrue(UseCase2.validateOrders(count));
        }

        @Test
        public void validateCustomers(){
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();
            String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
            Dataset<Row> customers =spark.read().format("csv").option("header",true).option("inferSchema",true).load(customersPath);
            long count=customers.count();
            Assert.assertTrue(UseCase2.validateCustomers(count));
        }

        @Test
        public void validateResult() {
            SparkSession spark = SparkSession.builder().master("local").getOrCreate();
            String ordersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
            String customersPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
            Dataset<Row> orders = spark.read().format("csv").option("header",true).option("inferSchema",true).load(ordersPath);
            Dataset<Row> customers =spark.read().format("csv").option("header",true).option("inferSchema",true).load(customersPath);
            Dataset<Row> result = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")),"right_outer").
                    where(orders.col("order_date").like("2014-01%").and(orders.col("order_id").isNull())).
                    select(customers.col("*")).
                    orderBy(customers.col("customer_id"));
            long count =result.count();
            Assert.assertTrue(UseCase2.validateResult(count));
        }
    }


