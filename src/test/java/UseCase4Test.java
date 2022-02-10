import org.junit.Assert;
import org.junit.Test;

public class UseCase4Test {
    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,UseCase4.getOrdersCount());
    }

    @Test
    public void validateOrder_items(){
        Assert.assertEquals(172198,UseCase4.getOrder_itemsCount());
    }

    @Test
    public void validateProducts(){
        Assert.assertEquals(1345,UseCase4.getProductsCount());
    }

    @Test
    public void validateCategories(){
        Assert.assertEquals(58,UseCase4.getCategoriesCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(33,UseCase4.getResultCount());
    }

}