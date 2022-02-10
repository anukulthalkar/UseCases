import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/*
Get the products for each department.
 * Tables - departments, categories, products
 * Data should be sorted in ascending order by department_id
 * Output should contain all the fields from department and the product count as product_count
 */
public class UseCase5 {

    static final Logger logger = Logger.getLogger(UseCase5.class);
    public static long getDepartmentsCount() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String departmentsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        Dataset<Row>departments=spark.read().format("csv").option("header",true).option("inferschema",true).load(departmentsPath);
        long departmentsCount = departments.count();
        return departmentsCount;
    }
    public static long getCategoriesCount() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String categoriesPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row>categories=spark.read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        long categoriesCount = categories.count();
        return categoriesCount;
    }
    public static long getProductsCount() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String productsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row>products=spark.read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        long productsCount = products.count();
        return productsCount;
    }
    public static long getResultCount() {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String departmentsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String categoriesPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        String productsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row>departments=spark.read().format("csv").option("header",true).option("inferschema",true).load(departmentsPath);
        Dataset<Row> categories=spark.read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        Dataset<Row>products=spark.read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        Dataset<Row>join1=departments.join(categories, departments.col("department_id").equalTo(categories.col("category_department_id")));
        Dataset<Row>result=join1.join(products, join1.col("category_id").equalTo(products.col("product_category_id"))).
                groupBy(join1.col("department_id"),
                        join1.col("department_name")).
                agg(count(products.col("product_category_id"))).
                orderBy(join1.col("department_id"));
        long resultCount = result.count();
        return resultCount;
    }
    public static void main(String[] args) {
        logger.info("------------------------------------------running UseCase 5------------------------------------------------------");

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("------------------------------------------spark session created--------------------------------------------------");

        String departmentsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String categoriesPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        String productsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row>departments=spark.read().format("csv").option("header",true).option("inferschema",true).load(departmentsPath);
        Dataset<Row> categories=spark.read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        Dataset<Row>products=spark.read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        departments.show();
        categories.show();
        products.show();
        Dataset<Row>join1=departments.join(categories, departments.col("department_id").equalTo(categories.col("category_department_id")));
        join1.show();
        Dataset<Row>result=join1.join(products, join1.col("category_id").equalTo(products.col("product_category_id"))).
                groupBy(join1.col("department_id"),
                        join1.col("department_name")).
                agg(count(products.col("product_category_id"))).
                orderBy(join1.col("department_id"));
        result.show(100);

        logger.info("------------------------------------------Write Result-----------------------------------------------------------");

        result.coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase5");

        logger.info("--------------------------------------------Completed------------------------------------------------------------");

    }
}