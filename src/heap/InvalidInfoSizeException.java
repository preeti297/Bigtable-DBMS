package heap;
import chainexception.*;

public class InvalidInfoSizeException extends ChainException{

   public InvalidInfoSizeException()
   {
      super();
   }
   
   public InvalidInfoSizeException(Exception ex, String name)
   {
      super(ex, name); 
   }

}

