import org.junit.Assert;
import org.junit.Test;

public class UseCase2Test {
    public static long getOrdersCount(){
        long resultCount = UseCase2.getOrders().count();
        return resultCount;
    }

    public static long getCustomersCount() {
        long customersCount = UseCase2.getCustomers().count();
        return customersCount;
    }

    public static long getUseCase2ResultCount() {
        long resultCount = UseCase2.getUseCase2Result().count();
        return resultCount;
    }

    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,getOrdersCount());
    }

    @Test
    public void validateCustomers(){
        Assert.assertEquals(12435,getCustomersCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(0,getUseCase2ResultCount());
    }

}