package bigt;

import java.io.IOException;
import java.util.*;

import bigt.Map;
import global.*;
import heap.*;
import index.IndexException;
import index.IndexScan;
import index.InvalidSelectionException;
import index.UnknownIndexTypeException;
import iterator.*;
import btree.*;
import bufmgr.PageNotReadException;

/**
 * 
 * Creation of a bigTable
 *
 */
public class BigT {

	public String name;
	public int type;
	public Heapfile _hf;
	public BTreeFile _bf0 = null;
	public BTreeFile _bf1 = null;
	public BTreeFile _bftemp = null;
	FileScan fscan;
	IndexScan iscan;
	CondExpr[] expr;
	AttrType[] attrType;
	short[] attrSize;
	String key;

	/**
	 * 
	 * @param name - name of the bigT
	 * @param type - type of the Database
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws GetFileEntryException
	 * @throws ConstructPageException
	 * @throws AddFileEntryException
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 */
	public BigT(String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException,
			GetFileEntryException, ConstructPageException, AddFileEntryException, FileScanException,
			TupleUtilsException, InvalidRelation, InvalidTypeException {

		this.name = name;
		this.type = type;
		this._hf = new Heapfile(name);

		switch (type) {
		case 1: {
			break;
		}

		case 2: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
			break;
		}

		case 3: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
			break;
		}

		case 4: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
			break;
		}
		case 5: {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
			break;
		}
		}

		attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);
		attrType[3] = new AttrType(AttrType.attrString);
		attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 22;
		attrSize[2] = 4;
		attrSize[3] = 22;

	}

	/**
	 * 
	 * Delete the bigt within the Database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws FileAlreadyDeletedException
	 * @throws InvalidInfoSizeException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws IteratorException
	 * @throws UnpinPageException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws ConstructPageException
	 * @throws PinPageException
	 */
	void deleteBigT() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidInfoSizeException,
			HFBufMgrException, HFDiskMgrException, IOException, IteratorException, UnpinPageException,
			FreePageException, DeleteFileEntryException, ConstructPageException, PinPageException {

		_hf.deleteFile();

		switch (type) {
		case 1: {
			break;
		}

		case 2: {
			_bf0.destroyFile();
			break;

		}

		case 3: {
			_bf0.destroyFile();
			break;

		}
		case 4: {
			_bf0.destroyFile();
			_bf1.destroyFile();
			break;
		}
		case 5: {
			_bf0.destroyFile();
			_bf1.destroyFile();
			break;
		}
		}
	}

	/**
	 * Initializing temporary index file to handle versioning
	 * 
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FileScanException
	 * @throws TupleUtilsException
	 * @throws InvalidRelation
	 * @throws InvalidTypeException
	 * @throws JoinsException
	 * @throws InvalidTupleSizeException
	 * @throws PageNotReadException
	 * @throws PredEvalException
	 * @throws UnknowAttrType
	 * @throws FieldNumberOutOfBoundException
	 * @throws WrongPermat
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 */

	public void populateBtree() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException, FileScanException, TupleUtilsException, InvalidRelation,
			InvalidTypeException, JoinsException, InvalidTupleSizeException, PageNotReadException, PredEvalException,
			UnknowAttrType, FieldNumberOutOfBoundException, WrongPermat, GetFileEntryException, AddFileEntryException {

		_bftemp = new BTreeFile(name + "Temp", AttrType.attrString, 64, 1);
		fscan = new FileScan(name, attrType, attrSize, (short) 4, 4, null, null);
		MapMidPair mpair = fscan.get_nextMidPair();
		while (mpair != null) {
			String  s = String.format("%06d", mpair.map.getTimeStamp());
			String key = mpair.map.getRowLabel() + mpair.map.getColumnLabel()+"%"+s;
			_bftemp.insert(new StringKey(key), mpair.mid);
//			_bftemp.insert(new StringKey(mpair.map.getRowLabel() + mpair.map.getColumnLabel()), mpair.mid);
			mpair = fscan.get_nextMidPair();
		}

		fscan.close();

	}

	/**
	 * Get the count of total number of maps
	 * 
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidInfoSizeException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws IOException
	 */
	public int getMapCnt() throws InvalidSlotNumberException, InvalidInfoSizeException, HFDiskMgrException,
			HFBufMgrException, IOException {
		return _hf.getRecCnt();
	}

	/**
	 * Gets the total number of unique rows in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               row count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getRowCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		Map map = new Map();
		fscan = new FileScan(name, attrType, attrSize, (short) 4, 4, null, null);
		Sort sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, new TupleOrder(TupleOrder.Ascending), 22, numbuf);
		int count = 0;
		map = sort.get_next();
		String prev = "";
		String curr = "";
		if (map != null) {
			prev = map.getRowLabel();
		}
		while (map != null) {
			curr = map.getRowLabel();
			if (!curr.equals(prev)) {
				count++;

			}
			prev = curr;

			map = sort.get_next();

		}
		if (curr.equals(prev)) {
			count++;

		}
		sort.close();
		System.out.println("Row count is " + count);
		return count;
	}

	/**
	 * 
	 * Gets the total number of unique columns in the database
	 * 
	 * @param numbuf - number of buffers used for sorting functionality to get the
	 *               column count
	 * @return
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public int getColumnCnt(int numbuf) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		Map map = new Map();
		fscan = new FileScan(name, attrType, attrSize, (short) 4, 4, null, null);
		Sort sort = new Sort(attrType, (short) 4, attrSize, fscan, 2, new TupleOrder(TupleOrder.Ascending), 22, numbuf);
		int count = 0;
		map = sort.get_next();
		String prev = "";
		String curr = "";
		if (map != null) {
			prev = map.getColumnLabel();

		}
		while (map != null) {
			curr = map.getColumnLabel();
			if (!curr.equals(prev)) {
				count++;

			}
			prev = curr;

			map = sort.get_next();

		}
		if (curr.equals(prev)) {
			count++;

		}
		sort.close();

		System.out.println("Col count is " + count);
		return count;
	}

	/**
	 * Generate the index files for the maps corresponding to the desired type
	 * 
	 * @throws KeyTooLongException
	 * @throws KeyNotMatchException
	 * @throws LeafInsertRecException
	 * @throws IndexInsertRecException
	 * @throws ConstructPageException
	 * @throws UnpinPageException
	 * @throws PinPageException
	 * @throws NodeNotMatchException
	 * @throws ConvertException
	 * @throws DeleteRecException
	 * @throws IndexSearchException
	 * @throws IteratorException
	 * @throws LeafDeleteException
	 * @throws InsertException
	 * @throws IOException
	 * @throws FieldNumberOutOfBoundException
	 * @throws InvalidInfoSizeException
	 * @throws FreePageException
	 * @throws DeleteFileEntryException
	 * @throws GetFileEntryException
	 * @throws AddFileEntryException
	 */
	public void insertIndex() throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
			NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
			LeafDeleteException, InsertException, IOException, FieldNumberOutOfBoundException, InvalidInfoSizeException,
			FreePageException, DeleteFileEntryException, GetFileEntryException, AddFileEntryException {
		Scan scan = new Scan(_hf);
		MID mid = new MID();
		String key = null;
		int key_timeStamp = 0;
		Map temp = null;

		if(type==1) {
			return;
		}
		if (!(type == 4 || type == 5)) {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 22, 1);
			
		} else {
			this._bf0 = new BTreeFile(name + "Index0", AttrType.attrString, 44, 1);
			this._bf1 = new BTreeFile(name + "Index1", AttrType.attrInteger, 4, 1);
		}

		temp = scan.getNext(mid);
		while (temp != null) {
			switch (type) {

			case 2: {
				key = temp.getRowLabel();
				_bf0.insert(new StringKey(key), mid);
				break;
			}

			case 3: {
				key = temp.getColumnLabel();
				_bf0.insert(new StringKey(key), mid);
				break;
			}

			case 4: {
				key = temp.getColumnLabel() + temp.getRowLabel();
				key_timeStamp = temp.getTimeStamp();
				_bf0.insert(new StringKey(key), mid);
				_bf1.insert(new IntegerKey(key_timeStamp), mid);
				break;
			}

			case 5: {
				key = temp.getRowLabel() + temp.getValue();
				key_timeStamp = temp.getTimeStamp();
				_bf0.insert(new StringKey(key), mid);
				_bf1.insert(new IntegerKey(key_timeStamp), mid);
				break;
			}

			}
			temp = scan.getNext(mid);
		}
		scan.closescan();
	}

	/**
	 * Initialize a map in the database
	 * 
	 * @param mapPtr
	 * @return
	 * @throws InvalidTypeException
	 * @throws IOException
	 */
	public Map constructMap(byte[] mapPtr) throws InvalidTypeException, IOException {
		Map map = new Map(mapPtr, 0);
		map.setHdr();
		return map;
	}

	/**
	 * Insert data given into bytes in the map
	 * 
	 * @param mapPtr - data to be inserted in bytes
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public MID insertMap(byte[] mapPtr) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
			HFBufMgrException, HFDiskMgrException, Exception {
		MID mid = _hf.insertRecord(mapPtr);
		return mid;
	}

	/**
	 * Remove Duplicates to handle the versioning in the database
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public void removeDuplicates()
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		TupleOrder[] order = new TupleOrder[2];
		order[0] = new TupleOrder(TupleOrder.Ascending);
		order[1] = new TupleOrder(TupleOrder.Descending);
		iscan = new IndexScan(new IndexType(IndexType.Row_Index), name, name + "Temp", attrType, attrSize, 4, 4, null,
				expr, 1, true);

		MapMidPair mpair = iscan.get_nextMidPair();
		String key = "";
		String oldKey = "";
		if (mpair != null) {
			oldKey = mpair.indKey.split("%")[0];
		}
		List<MID> list = new ArrayList<MID>();
		while (mpair != null) {

			key = mpair.indKey.split("%")[0];

			if (key.equals(oldKey)) {
				list.add(mpair.mid);
			} else {
				list.clear();
				oldKey = key;
				list.add(mpair.mid);
			}

			if (list.size() == 4) {
				MID delmid = list.get(0);

				_hf.deleteRecord(delmid);
				list.remove(0);
			}

			mpair = iscan.get_nextMidPair();
		}
		iscan.close();
		_bftemp.destroyFile();

	}

	/**
	 * Fucntion to display all the maps in the database
	 * 
	 * @throws InvalidInfoSizeException
	 * @throws IOException
	 */
	public void scanAllMaps() throws InvalidInfoSizeException, IOException {
		Scan scan = _hf.openScan();
		MID mid = new MID();
		Map map = scan.getNext(mid);
		System.out.println("Maps are ");
		while (map != null) {
			map.print();
			System.out.println();
			map = scan.getNext(mid);
		}
	}

	/**
	 * Utility for purpose of testing
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public void generateMapsAndIndex() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
			HFBufMgrException, HFDiskMgrException, Exception {
		String row[] = new String[] { "aa", "kkdab", "kk", "zz", "ee", "cc", "kk", "ff", "tt", "uu", "kkdaa", "kkdba",
				"cc", "kkdaa", "aa", "kk", "kk" };
		String col[] = new String[] { "e", "daaab", "a", "b", "d", "p", "a", "a", "e", "u", "daaa", "p", "s", "f", "b",
				"a", "b" };
		String val[] = new String[] { "america", "zzzzz", "ksks", "india", "china", "japan", "pakistan", "korea", "ggs",
				"australia", "kl", "zimbambwe", "bfgjh", "antarctica", "LKL", "pou", "Italy" };

		for (int i = 0; i < row.length; i++) {
			Map newmap = new Map();
			newmap.setHdr();
			newmap.setRowLabel(row[i]);
			newmap.setColumnLabel(col[i]);
			newmap.setTimeStamp(i);
			newmap.setValue(val[i]);
			insertMap(newmap.getMapByteArray());

		}
	}

	/**
	 * Opens the stream of maps
	 * 
	 * @param orderType - Desired order of Results
	 * @param rowFilter - Filtering condition on row
	 * @param colFilter - Filtering condition on column
	 * @param valFilter - Filtering condition on value
	 * @param numbuf    - number of buffers allocated
	 * @return
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 */
	public Stream openStream(int orderType, String rowFilter, String colFilter, String valFilter, int numbuf)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Stream stream = new Stream(this, orderType, rowFilter, colFilter, valFilter, numbuf);
		return stream;

	}

}