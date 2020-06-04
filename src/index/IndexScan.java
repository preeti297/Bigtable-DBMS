package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import btree.*;
import iterator.*;
import heap.*;
import java.io.*;
import bigt.*;
import iterator.*;

/**
 * Index Scan iterator will directly access the required tuple using the
 * provided key. It will also perform selections and projections. information
 * about the tuples and the index are passed to the constructor, then the user
 * calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

	/**
	 * class constructor. set up the index scan.
	 * 
	 * @param index     type of the index (B_Index, Hash)
	 * @param relName   name of the input relation
	 * @param indName   name of the input index
	 * @param types     array of types in this relation
	 * @param str_sizes array of string sizes (for attributes that are string)
	 * @param noInFlds  number of fields in input tuple
	 * @param noOutFlds number of fields in output tuple
	 * @param outFlds   fields to project
	 * @param selects   conditions to apply, first one is primary
	 * @param fldNum    field number of the indexed field
	 * @param indexOnly whether the answer requires only the key or the tuple
	 * @exception IndexException            error from the lower layer
	 * @exception InvalidTypeException      tuple type not valid
	 * @exception InvalidTupleSizeException tuple size not valid
	 * @exception UnknownIndexTypeException index type unknown
	 * @exception IOException               from the lower layer
	 * @throws ConstructPageException
	 * @throws IteratorException
	 * @throws PinPageException
	 * @throws UnpinPageException
	 * @throws KeyNotMatchException
	 * @throws InvalidSelectionException
	 * @throws UnknownKeyTypeException
	 * @throws GetFileEntryException
	 */
	public IndexScan(IndexType index, final String relName, final String indName, AttrType types[], short str_sizes[],
			int noInFlds, int noOutFlds, FldSpec outFlds[], CondExpr selects[], final int fldNum,
			final boolean indexOnly)
			throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
			IOException, UnknownKeyTypeException, InvalidSelectionException, KeyNotMatchException, UnpinPageException,
			PinPageException, IteratorException, ConstructPageException, GetFileEntryException {
		// set them to 4
		// noOutFlds = 4;
		// noInFlds = 4;
		_fldNum = fldNum;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;

		// Number of output fields
		AttrType[] Jtypes = new AttrType[noOutFlds];
		short[] ts_sizes;
		Map JMap = new Map();

		// try {
		// ts_sizes = MapUtils.setup_op_tuple(JMap, Jtypes, types, noInFlds, str_sizes,
		// outFlds, noOutFlds);
		// }
		// catch (TupleUtilsException e) {
		// throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from
		// TupleUtils.setup_op_tuple()");
		// }
		// catch (InvalidRelation e) {
		// throw new IndexException(e, "IndexScan.java: InvalidRelation caught from
		// TupleUtils.setup_op_tuple()");
		// }

		_selects = selects;
		perm_mat = outFlds;
		_noOutFlds = noOutFlds;
		Map map1 = new Map();
		try {
			map1.setHdr();
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile error");
		}

		m1_size = map1.size();
		index_only = indexOnly; // added by bingjie miao

		try {
			f = new Heapfile(relName);
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile not created");
		}

		switch (index.indexType) {

		case IndexType.Row_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Col_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		
		case IndexType.Col_Row_index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Row_Value_index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Time_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		

		case IndexType.No_Index:
		default:
			throw new UnknownIndexTypeException("Only BTree index is supported so far");

		}

	}

	public IndexScan(IndexType index, final String relName, final String indName, AttrType types[], short str_sizes[],
			int noInFlds, int noOutFlds, FldSpec outFlds[], CondExpr selects[], final int fldNum,
			final boolean indexOnly, CondExpr evalSelects[])
			throws IndexException, InvalidTypeException, InvalidTupleSizeException, UnknownIndexTypeException,
			IOException, UnknownKeyTypeException, InvalidSelectionException, KeyNotMatchException, UnpinPageException,
			PinPageException, IteratorException, ConstructPageException, GetFileEntryException {
		// set them to 4
		// noOutFlds = 4;
		// noInFlds = 4;
		_fldNum = fldNum;
		_noInFlds = noInFlds;
		_types = types;
		_s_sizes = str_sizes;

		// Number of output fields
		AttrType[] Jtypes = new AttrType[noOutFlds];
		short[] ts_sizes;
		Map JMap = new Map();

		// try {
		// ts_sizes = MapUtils.setup_op_tuple(JMap, Jtypes, types, noInFlds, str_sizes,
		// outFlds, noOutFlds);
		// }
		// catch (TupleUtilsException e) {
		// throw new IndexException(e, "IndexScan.java: TupleUtilsException caught from
		// TupleUtils.setup_op_tuple()");
		// }
		// catch (InvalidRelation e) {
		// throw new IndexException(e, "IndexScan.java: InvalidRelation caught from
		// TupleUtils.setup_op_tuple()");
		// }

		_selects = selects;
		_eval_selects = evalSelects;
		perm_mat = outFlds;
		_noOutFlds = noOutFlds;
		Map map1 = new Map();
		try {
			map1.setHdr();
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile error");
		}

		m1_size = map1.size();
		index_only = indexOnly; // added by bingjie miao

		try {
			f = new Heapfile(relName);
		} catch (Exception e) {
			throw new IndexException(e, "IndexScan.java: Heapfile not created");
		}

		switch (index.indexType) {

		case IndexType.Row_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Col_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		
		case IndexType.Col_Row_index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Row_Value_index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		
		case IndexType.Time_Index: {
			indFile = new BTreeFile(indName);
			indScan = (BTFileScan) IndexUtils.BTree_scan(selects, indFile);
			break;
		}
		

		case IndexType.No_Index:
		default:
			throw new UnknownIndexTypeException("Only BTree index is supported so far");

		}

	}
	
	
	/**
	 * returns the next tuple. if <code>index_only</code>, only returns the key
	 * value (as the first field in a tuple) otherwise, retrive the tuple and
	 * returns the whole tuple
	 * 
	 * @return the tuple
	 * @throws Exception
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws HFException
	 * @throws InvalidTupleSizeException
	 * @throws InvalidSlotNumberException
	 */
	public Map get_next() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException,
			HFBufMgrException, Exception {
		MID mid;
		int unused;
		KeyDataEntry nextentry = null;
		nextentry = indScan.get_next();
		
		while (nextentry != null) {
			
			
		    if (index_only) {
		    	// only need to return the key 

		    	AttrType[] attrType = new AttrType[1];
		    	short[] s_sizes = new short[1];
		    	
		    	
		    	Map Jmap=new Map();
//		    	String str = "";
		    	  
		    	  attrType[0] = new AttrType(AttrType.attrString);
		    	  // calculate string size of _fldNum
		    	  int count = 0;
		    	  for (int i=0; i<_fldNum; i++) {
		    	    if (_types[i].attrType == AttrType.attrString)
		    	      count ++;
		    	  } 
		    	  s_sizes[0] = _s_sizes[count-1];
		    	  
		    	  try {
		    		  Jmap.setHdr();
		    	  }
		    	  catch (Exception e) {
		    	    throw new IndexException(e, "IndexScan.java: Heapfile error");
		    	  }
		    	  
		    	  try {
//		    		str = ((StringKey)nextentry.key).getKey();
		    		  
		    		  Jmap.setValue(((StringKey)nextentry.key).getKey());
		    	  }
		    	  catch (Exception e) {
		    	    throw new IndexException(e, "IndexScan.java: Heapfile error");
		    	  }	  
		    	
		    
		    	return Jmap;
		          }
			
			mid = ((LeafData) nextentry.data).getData();
			Map1 = f.getRecord(mid);
			Map1.setHdr();
			
			//System.out.println("Map1 in iscan is "+Map1.getRowLabel());
			
			boolean eval;
			eval = PredEval.Eval(_eval_selects, Map1, null, _types, null);
			
			
			//System.out.println("eval is false");
			if (eval) {
				//no  need for  projection.java
				JMap = Map1;
				// Projection.Project(Map1, _types, JMap, perm_mat, _noOutFlds);
				return JMap;
			}
			
			nextentry = indScan.get_next();
		}

		return null;
	}
	
	
	  
		public MapMidPair get_nextMidPair() throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

			MID mid;
			int unused;
			KeyDataEntry nextentry = null;
			nextentry = indScan.get_next();
			
			while (nextentry != null) {
				
				if (index_only) {
			    	// only need to return the key 

			    	AttrType[] attrType = new AttrType[1];
			    	short[] s_sizes = new short[1];
			    	
			    	MapMidPair Jpair = new MapMidPair();
			    	
//			    	String str = "";
			    	  
			    	  attrType[0] = new AttrType(AttrType.attrString);
			    	  // calculate string size of _fldNum
			    	  int count = 0;
			    	  for (int i=0; i<_fldNum; i++) {
			    	    if (_types[i].attrType == AttrType.attrString)
			    	      count ++;
			    	  } 
			    	  s_sizes[0] = _s_sizes[count-1];
			    
			    		Jpair.indKey = ((StringKey)nextentry.key).getKey();
			    		  
			    		mid = ((LeafData) nextentry.data).getData();
			    	Jpair.mid = mid;
			    
			    	return Jpair;
			          }
				
				mid = ((LeafData) nextentry.data).getData();
				Map1 = f.getRecord(mid);
				Map1.setHdr();
				
				//System.out.println("Map1 in index scan is ");
				//Map1.print();
				//System.out.println();
				
				boolean eval;
				eval = PredEval.Eval(_eval_selects, Map1, null, _types, null);
				
				
				
				//System.out.println("eval is false");
				if (eval) {
					//no  need for  projection.java
					JMap = Map1;
					// Projection.Project(Map1, _types, JMap, perm_mat, _noOutFlds);
					MapMidPair mpair = new MapMidPair();
					mpair.map = JMap;
					mpair.mid = mid;
					return mpair;
				}
				
				nextentry = indScan.get_next();
			}

			return null;
		}


	/**
	 * Cleaning up the index scan, does not remove either the original relation or
	 * the index from the database.
	 * 
	 * @exception IndexException error from the lower layer
	 * @exception IOException    from the lower layer
	 */
	public void close() throws IOException, IndexException {
		if (!closeFlag) {
			if (indScan instanceof BTFileScan) {
				try {
					((BTFileScan) indScan).DestroyBTreeFileScan();
				} catch (Exception e) {
					throw new IndexException(e, "BTree error in destroying index scan.");
				}
			}

			closeFlag = true;
		}
	}
	
	

	public FldSpec[] perm_mat;
	private IndexFile indFile;
	private IndexFileScan indScan;
	private AttrType[] _types;
	private short[] _s_sizes;
	private CondExpr[] _selects;
	private CondExpr[] _eval_selects;
	private int _noInFlds;
	private int _noOutFlds;
	private Heapfile f;
	private Map Map1;
	private Map JMap;
	private int m1_size;
	private int _fldNum;
	private boolean index_only;

}
