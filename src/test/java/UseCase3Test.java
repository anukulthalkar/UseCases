import org.junit.Assert;
import org.junit.Test;

public class UseCase3Test {

    public static long getOrdersCount(){
        long ordersCount = UseCase3.getOrders().count();
        return ordersCount;
    }

    public static long getCustomersCount() {
        long customersCount = UseCase3.getCustomers().count();
        return customersCount;
    }

    public static long getOrder_itemsCount() {
        long order_itemsCount = UseCase3.getOrder_items().count();
        return order_itemsCount;
    }

    public static long getResultCount() {
        long resultCount = UseCase3.getUseCase3Result().count();
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
    public void validateOrder_items(){
        Assert.assertEquals(172198,getOrder_itemsCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(1941,getResultCount());
    }

}