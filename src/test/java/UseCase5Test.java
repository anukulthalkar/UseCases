import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.spark.sql.functions.count;

public class UseCase5Test {
    @Test
    public void validateResult(){
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        String departmentsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
        String categoriesPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        String productsPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> departments=spark.read().format("csv").option("header",true).option("inferschema",true).load(departmentsPath);
        Dataset<Row> categories=spark.read().format("csv").option("header",true).option("inferschema",true).load(categoriesPath);
        Dataset<Row>products=spark.read().format("csv").option("header",true).option("inferschema",true).load(productsPath);
        Dataset<Row>join1=departments.join(categories, departments.col("department_id").equalTo(categories.col("category_department_id")));
        Dataset<Row>result=join1.join(products, join1.col("category_id").equalTo(products.col("product_category_id"))).
                groupBy(join1.col("department_id"),
                        join1.col("department_name")).
                agg(count(products.col("product_category_id"))).
                orderBy(join1.col("department_id"));
        long count = result.count();
        Assert.assertTrue(UseCase5.validateResult(count));
    }
}
