import org.junit.Assert;
import org.junit.Test;

public class UseCase1Test {
    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,UseCase1.getOrdersCount());
    }

    @Test
    public void validateCustomers(){
        Assert.assertEquals(12435,UseCase1.getCustomersCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(4696,UseCase1.getResultCount());
    }

}