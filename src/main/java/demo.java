
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class demo {
    public static void main(String[] args){
        String orderPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        SparkSession spark= SparkSession.builder().master("local").getOrCreate();
        Dataset<Row> orders=spark.read().format("csv").option("header", true).option("inferSchema",true).load(orderPath);
        orders.show();
    }
}
