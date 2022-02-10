import org.junit.Assert;
import org.junit.Test;

public class UseCase5Test {
    @Test
    public void validateDepartments(){
        Assert.assertEquals(6,UseCase5.getDepartmentsCount());
    }

    @Test
    public void validateCategories(){
        Assert.assertEquals(58,UseCase5.getCategoriesCount());
    }

    @Test
    public void validateProducts(){
        Assert.assertEquals(1345,UseCase5.getProductsCount());
    }

    @Test
    public void validateResult(){
        Assert.assertEquals(6,UseCase5.getResultCount());
    }

}