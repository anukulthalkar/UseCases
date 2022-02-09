
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class demo {
    static  long result;
    public static long getResult(){
        SparkSession spark= SparkSession.builder().master("local").getOrCreate();
        String ordersPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders=spark.read().format("csv").option("header", true).option("inferSchema",true).load(ordersPath);
        result=orders.count();
        return result;
    }

    public static void main(String[] args){
        getResult();
        System.out.println(result);
    }
}
