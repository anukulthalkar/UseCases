import org.junit.Assert;
import org.junit.Test;

public class UseCase1Test {
    public static long getOrdersCount(){
        long ordersCount = UseCase1.getOrders().count();
        return ordersCount;
    }

    public static long getCustomersCount() {
        long customersCount = UseCase1.getCustomers().count();
        return customersCount;
    }

    public static long getResultCount() {
        long resultCount = UseCase1.getUseCase1Result().count();
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
        Assert.assertEquals(4696,getResultCount());
    }

}