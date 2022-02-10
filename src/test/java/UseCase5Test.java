import org.junit.Assert;
import org.junit.Test;

public class UseCase5Test {

    public static long getDepartmentsCount() {
        long departmentsCount = UseCase5.getDepartments().count();
        return departmentsCount;
    }

    public static long getCategoriesCount() {
        long categoriesCount = UseCase5.getCategories().count();
        return categoriesCount;
    }

    public static long getProductsCount() {
        long productsCount = UseCase5.getProducts().count();
        return productsCount;
    }

    public static long getResultCount() {
        long resultCount = UseCase5.getUseCase5Result().count();
        return resultCount;
    }

    @Test
    public void validateDepartments(){
        Assert.assertEquals(6,getDepartmentsCount());
    }

    @Test
    public void validateCategories(){
        Assert.assertEquals(58,getCategoriesCount());
    }

    @Test
    public void validateProducts(){
        Assert.assertEquals(1345,getProductsCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(6,getResultCount());
    }

}