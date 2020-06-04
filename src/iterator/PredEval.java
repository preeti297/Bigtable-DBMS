package iterator;

import heap.*;
import global.*;
import java.io.*;
import bigt.*;

public class PredEval
{
  /**
   *predicate evaluate, according to the condition ConExpr, judge if 
   *the two map can join. if so, return true, otherwise false
   *@return true or false
   *@param p[] single select condition array
   *@param t1 compared map1
   *@param t2 compared map2
   *@param in1[] the attribute type corespond to the t1
   *@param in2[] the attribute type corespond to the t2
   *@exception IOException  some I/O error
   *@exception UnknowAttrType don't know the attribute type
   *@exception InvalidMapSizeException size of map not valid
   *@exception InvalidTypeException type of map not valid
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception PredEvalException exception from this method
   */
  public static boolean Eval(CondExpr p[], Map t1, Map t2, AttrType in1[], 
			     AttrType in2[])
    throws IOException,
	   UnknowAttrType,
	   InvalidTypeException,
	   FieldNumberOutOfBoundException,
	   PredEvalException
    {
      CondExpr temp_ptr;
      int       i = 0;
      Map    map1 = null, map2 = null;
      int      fld1, fld2;
     
      short[]     str_size = new short[1];
      AttrType[]  val_type = new AttrType[1];
      
      AttrType  comparison_type = new AttrType(AttrType.attrInteger);
      int       comp_res;
      boolean   op_res = false, row_res = false, col_res = true;
      
      if (p == null)
	{
	  return true;
	}
      
      
      //Create a new value (Map) and based on the offset in RelSpec ci=opy the only attribute in value and then compare 
      
      
      Map value = new Map();
      value.setHdr();
      
      
      
      while ( i<p.length-1 || p[i] != null )
      {
    	  if(p[i]==null)
    	  {
    		  i++;
    		  continue;
    	  }
    	  
		  temp_ptr = p[i];
		  
		  while (temp_ptr != null)
		    {
//		      val_type[0] = new AttrType(temp_ptr.type1.attrType);
//		      fld1        = 1;
			  
			 int type = p[i].operand1.symbol.offset;
			 fld1 = type;
			 
			 
		      switch (type)
		      {
		      
				case 1:
				  value.setRowLabel(temp_ptr.operand2.string);
				  map1 = value;
				  comparison_type.attrType = AttrType.attrString;
				  break;
				case 2:
					value.setColumnLabel(temp_ptr.operand2.string);
					map1 = value;
					comparison_type.attrType = AttrType.attrString;
				  break;
				  
				case 3:
					value.setTimeStamp(temp_ptr.operand2.integer);
					map1 = value;
					comparison_type.attrType = AttrType.attrInteger;
				  break;
				  
				case 4:
					value.setValue(temp_ptr.operand2.string);
					map1 = value;
					comparison_type.attrType = AttrType.attrString;
				  break;
				  
				case 5:
					value.setValue(temp_ptr.operand2.string);
					map1 = value;
					comparison_type.attrType = AttrType.attrString;
				  break;
				  
				case 6:
					value.setValue(temp_ptr.operand2.string);
					map1 = value;
					comparison_type.attrType = AttrType.attrString;
				  break;
				  
				default:
				  break;
			}
	      
	      /*
	      // Setup second argument for comparison.
	      val_type[0] = new AttrType(temp_ptr.type2.attrType);
	      fld2        = 1;
	      switch (temp_ptr.type2.attrType)
		{
		case AttrType.attrInteger:
		  value.setHdr((short)1, val_type, null);
		  value.setIntFld(1, temp_ptr.operand2.integer);
		  map2 = value;
		  break;
		case AttrType.attrReal:
		  value.setHdr((short)1, val_type, null);
		  value.setFloFld(1, temp_ptr.operand2.real);
		  map2 = value;
		  break;
		case AttrType.attrString:
		  str_size[0] = (short)(temp_ptr.operand2.string.length()+1 );
		  value.setHdr((short)1, val_type, str_size);
		  value.setStrFld(1, temp_ptr.operand2.string);
		  map2 = value;
		  break;
		case AttrType.attrSymbol:
		  fld2 = temp_ptr.operand2.symbol.offset;
		  if (temp_ptr.operand2.symbol.relation.key == RelSpec.outer)
		    map2 = t1;
		  else
		    map2 = t2;
		  break;
		default:
		  break;
		}
	      */
		      
//		      System.out.println("In Pred eval ");
//		      System.out.println("map from index scan is ");
//		      t1.print();
//		      System.out.println();
//		      System.out.println("Map based on filtering is ");
//		      map1.print();
//		      System.out.println();
		      
		      //System.out.println("t1 in eval is "+t1.getRowLabel()+" value1 in eval is "+map1.getRowLabel());
	      // Got the arguments, now perform a comparison.
	      try {
		comp_res = Maputils.CompareMapWithMap(comparison_type, t1, map1, fld1);
	      }catch (MapUtilsException e){
		throw new PredEvalException (e,"MapUtilsException is caught by PredEval.java");
	      }
	      op_res = false;
	      
	      switch (temp_ptr.op.attrOperator)
		{
		case AttrOperator.aopEQ:
		  if (comp_res == 0) op_res = true;
		  break;
		case AttrOperator.aopLT:
		  if (comp_res <  0) op_res = true;
		  break;
		case AttrOperator.aopGT:
		  if (comp_res >  0) op_res = true;
		  break;
		case AttrOperator.aopNE:
		  if (comp_res != 0) op_res = true;
		  break;
		case AttrOperator.aopLE:
		  if (comp_res <= 0) op_res = true;
		  break;
		case AttrOperator.aopGE:
		  if (comp_res >= 0) op_res = true;
		  break;
		case AttrOperator.aopNOT:
		  if (comp_res != 0) op_res = true;
		  break;
		default:
		  break;
		}
	      
	      row_res = row_res || op_res;
	      if (row_res == true)
		break;                        // OR predicates satisfied.
	      temp_ptr = temp_ptr.next;
	    }
	  i++;
	  
	  col_res = col_res && row_res;
	  if (col_res == false)
	    {
	      
	      return false;
	    }
	  row_res = false;                        // Starting next row.
	}
      
//      System.out.println("--------> Conditional expression is true the map getting returned is ");
//      map1.print();
//      System.out.println();
//      System.out.println("Map given as input is ");
//      t1.print();
//      System.out.println();
      return true;
      
    }
}

