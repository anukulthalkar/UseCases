import org.apache.spark.sql.SparkSession;
public class util {
    public static SparkSession getSparkSession(){
        SparkSession spark= SparkSession.builder().master("local").getOrCreate();
        return spark;
    }
}
