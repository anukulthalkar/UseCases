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

    public static Dataset<Row> getDepartments() {
        String departmentsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        Dataset<Row>departments=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(departmentsPath);
        return departments;
    }

    public static Dataset<Row> getCategories() {
        String categoriesPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row>categories=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        return categories;
    }

    public static Dataset<Row> getProducts() {
        String productsPath="C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row>products=util.getSparkSession().read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        return products;
    }

    public static Dataset<Row> getUseCase5Result() {
        Dataset<Row> departments =getDepartments();
        Dataset<Row> categories =getCategories();
        Dataset<Row> products = getProducts();
        Dataset<Row>join1=departments.join(categories, departments.col("department_id").equalTo(categories.col("category_department_id")));
        Dataset<Row>result=join1.join(products, join1.col("category_id").equalTo(products.col("product_category_id"))).
                groupBy(join1.col("department_id"),
                        join1.col("department_name")).
                agg(count(products.col("product_category_id"))).
                orderBy(join1.col("department_id"));
        return result;
    }

    public static void main(String[] args) {

        logger.info("------------------------------------------running UseCase 5------------------------------------------------------");

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();

        logger.info("------------------------------------------spark session created--------------------------------------------------");

        getDepartments().show();
        getDepartments().printSchema();
        getCategories().show();
        getCategories().printSchema();
        getProducts().show();
        getProducts().printSchema();
        getUseCase5Result().show();
        getUseCase5Result().printSchema();

        logger.info("------------------------------------------Write Result-----------------------------------------------------------");

        getUseCase5Result().coalesce(1).write().option("header",true).mode("overwrite").csv("C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\outputs\\UseCase5");

        logger.info("--------------------------------------------Completed------------------------------------------------------------");

    }
}