package btree;
import global.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "rid" for leaf node in B++ tree.
 */
public class LeafData extends DataClass {
  private MID myMid;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myMid.pageNo.pid)).toString() +" "
              + (new Integer(myMid.slotNo)).toString() + " ]";
     return s;
  }

  /** Class constructor
   *  @param    mid  the data mid
   */
  LeafData(MID mid) {myMid= new MID(mid.pageNo, mid.slotNo);};  

  /** get a copy of the rid
  *  @return the reference of the copy 
  */
  public MID getData() {return new MID(myMid.pageNo, myMid.slotNo);};

  /** set the rid
   */ 
  public void setData(MID mid) { myMid= new MID(mid.pageNo, mid.slotNo);};
}   
