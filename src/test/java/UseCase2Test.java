import org.junit.Assert;
import org.junit.Test;

public class UseCase2Test {
    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,UseCase2.getOrdersCount());
    }

    @Test
    public void validateCustomers(){
        Assert.assertEquals(12435,UseCase2.getCustomersCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(0,UseCase2.getUseCase2ResultCount());
    }

}