import org.junit.Assert;
import org.junit.Test;

public class UseCase1Test {
    @Test
    public void testcase(){
        UseCase1.main("Test");
        Assert.assertEquals(UseCase1.a,4696);
    }
}
