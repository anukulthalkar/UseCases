import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class demo {

    public static Dataset<Row> getOrders(){
        String ordersPath = "C:\\Users\\Anukul Thalkar\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders=util.getSparkSession().read().format("csv").option("header", true).option("inferSchema",true).load(ordersPath);
        return orders;
    }
    public static long getcount(){
        long result=getOrders().count();
        return result;
    }

    public static void main(String[] args){
        getOrders().show();
    }
}
