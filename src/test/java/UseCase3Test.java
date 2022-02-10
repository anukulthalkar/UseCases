import org.junit.Assert;
import org.junit.Test;

public class UseCase3Test {
    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,UseCase3.getOrdersCount());
    }

    @Test
    public void validateCustomers(){
        Assert.assertEquals(12435,UseCase3.getCustomersCount());
    }

    @Test
    public void validateOrder_items(){
        Assert.assertEquals(172198,UseCase3.getOrder_itemsCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(1941,UseCase3.getResultCount());
    }

}