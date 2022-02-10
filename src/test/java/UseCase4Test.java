import org.junit.Assert;
import org.junit.Test;

public class UseCase4Test {

    public static long getOrdersCount(){
        long ordersCount = UseCase4.getOrders().count();
        return ordersCount;
    }

    public static long getOrder_itemsCount() {
        long order_itemsCount = UseCase4.getOrder_items().count();
        return order_itemsCount;
    }

    public static long getProductsCount() {
        long productsCount = UseCase4.getProducts().count();
        return productsCount;
    }

    public static long getCategoriesCount() {
        long categoriesCount = UseCase4.getCategories().count();
        return categoriesCount;
    }

    public static long getResultCount() {

        long resultCount = UseCase4.getUseCase4Result().count();
        return resultCount;
    }
    @Test
    public void validateOrders(){
        Assert.assertEquals(68883,getOrdersCount());
    }

    @Test
    public void validateOrder_items(){
        Assert.assertEquals(172198,getOrder_itemsCount());
    }

    @Test
    public void validateProducts(){
        Assert.assertEquals(1345,getProductsCount());
    }

    @Test
    public void validateCategories(){
        Assert.assertEquals(58,getCategoriesCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(33,getResultCount());
    }

}