package global;

/** 
 * Enumeration class for IndexType
 * 
 */

public class IndexType {

  public static final int No_Index    = 1;
  public static final int Row_Index = 2;
  public static final int Col_Index    = 3;
  public static final int Col_Row_index  = 4;
  public static final int Row_Value_index  = 5;
  public static final int Time_Index  = 6;



  public int indexType;

  /** 
   * IndexType Constructor
   * <br>
   * An index type can be defined as 
   * <ul>
   * <li>   IndexType indexType = new IndexType(IndexType.Hash);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (indexType.indexType == IndexType.Hash) ....
   * </ul>
   *
   * @param _indexType The possible types of index
   */

  public IndexType (int _indexType) {
    indexType = _indexType;
  }

    public String toString() {

    switch (indexType) {
    case No_Index :
      return "No indexing";
    case Row_Index:
      return "row indexing";
    case Col_Index:
      return "Column indexing";
    case Col_Row_index:
      return "One indexing on Column and row and timestamp";
    case Row_Value_index:
      return "Row,value and timestamp";

    }
    return ("Unexpected IndexType " + indexType);
  }
}
