/* File Map.java */

package bigt;

import java.io.*;
import global.*;
import heap.*;

public class Map implements GlobalConst {

	/**
	 * Maximum size of any map
	 */
	public static final int max_size = MINIBASE_PAGESIZE;

	/**
	 * a byte array to hold data
	 */
	private byte[] data;

	/**
	 * start position of this map in data[]
	 */
	private int map_offset;

	/**
	 * length of this map metadata(6 [fldCnt, fldOffset[4], pointer to end of Map]
	 * *2) + 22(row length) + 22(col length) + 4(timstamp length) + 22(value length)
	 */
	private int map_length = 82;

	/**
	 * private field Number of fields in this map
	 */
	private short fldCnt = 4;

	/**
	 * private field Array of offsets of the fields
	 */

	private short[] fldOffset;

	/**
	 * Class constructor Create a new map with length = max_size,map offset = 0.
	 */

	public Map() {
		data = new byte[max_size];
		map_offset = 0;
		map_length = max_size;
	}

	/**
	 * Constructor
	 * 
	 * @param amap   a byte array which contains the map
	 * @param offset the offset of the map in the byte array
	 * @param length the length of the map
	 */

	public Map(byte[] amap, int offset) {
		data = amap;
		map_offset = offset;

	}

	/**
	 * Constructor(used as map copy)
	 * 
	 * @param fromMap a byte array which contains the map
	 * 
	 */
	public Map(Map fromMap) {
		data = fromMap.getMapByteArray();
		map_length = fromMap.getLength();
		map_offset = 0;
		fldCnt = fromMap.noOfFlds();
		fldOffset = fromMap.copyFldOffset();
	}

	/**
	 * Class constructor Create a new map with length = size,map offset = 0.
	 * 
	 * @param size - size of the map
	 */

	public Map(int size) {
		// Create a new map
		data = new byte[size];
		map_offset = 0;
		map_length = size;
	}

	/**
	 * Copy a map to the current map position
	 * 
	 * @param fromMap the map being copied
	 */
	public void mapCopy(Map fromMap) {
		byte[] temparray = fromMap.getMapByteArray();
		System.arraycopy(temparray, 0, data, map_offset, map_length);
	}

	/**
	 * This is used when you don't want to use the constructor
	 * 
	 * @param amap   a byte array which contains the map
	 * @param offset the offset of the map in the byte array
	 * @param length the length of the map
	 */

	public void mapInit(byte[] amap, int offset, int length) {
		data = amap;
		map_offset = offset;
		map_length = length;

	}

	/**
	 * Set a map with the given map length and offset
	 * 
	 * @param record a byte array contains the map
	 * @param offset the offset of the map ( =0 by default)
	 * @param length the length of the map
	 */
	public void mapSet(byte[] record, int offset, int length) {
		System.arraycopy(record, offset, data, 0, length);
		map_offset = 0;
		map_length = length;
	}

	/**
	 * get the length of a map, call this method if you did not call setHdr ()
	 * before
	 * 
	 * @return length of this map in bytes
	 */
	public int getLength() {
		return map_length;
	}

	/**
	 * get the length of a map, call this method if you did call setHdr () before
	 * 
	 * @return size of this map in bytes
	 */
	public short size() {
		return ((short) (fldOffset[fldCnt] - map_offset));
	}

	/**
	 * get the offset of a map
	 * 
	 * @return offset of the map in byte array
	 */
	public int getOffset() {
		return map_offset;
	}

	/**
	 * Copy the map byte array out
	 * 
	 * @return byte[], a byte array contains the map the length of byte[] = length
	 *         of the map
	 */

	public byte[] getMapByteArray() {
		byte[] mapcopy = new byte[map_length];
		System.arraycopy(data, map_offset, mapcopy, 0, map_length);
		return mapcopy;
	}

	/**
	 * return the data byte array
	 * 
	 * @return data byte array
	 */

	public byte[] returnMapByteArray() {
		return data;
	}

	/**
	 * Convert row field into string
	 * 
	 * @return the converted string if success
	 * 
	 * @exception IOException I/O errors
	 */

	public String getRowLabel() throws IOException {

		String val = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]);
		return val;
	}

	/**
	 * Convert column field into string
	 * 
	 * @return the converted string if success
	 * 
	 * @exception IOException I/O errors
	 */

	public String getColumnLabel() throws IOException {
		String val = Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]);
		return val;
	}

	/**
	 * Convert time stamp field into integer
	 * 
	 * @return the converted integer if success
	 * 
	 * @exception IOException I/O errors
	 */

	public int getTimeStamp() throws IOException {

		int val = Convert.getIntValue(fldOffset[2], data);
		return val;
	}

	/**
	 * Convert value field into integer
	 * 
	 * @return the converted integer if success
	 * 
	 * @exception IOException I/O errors
	 */

	public String getValue() throws IOException {

		String val = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
		return val;
	}

	/**
	 * Set row field to string value
	 *
	 * @param val the string value
	 * @exception IOException I/O errors
	 */

	public Map setRowLabel(String val) throws IOException {

		Convert.setStrValue(val, fldOffset[0], data);
		return this;
	}

	/**
	 * Set column field to string value
	 *
	 * @param val the string value
	 * @exception IOException I/O errors
	 */

	public Map setColumnLabel(String val) throws IOException {

		Convert.setStrValue(val, fldOffset[1], data);
		return this;
	}

	/**
	 * Set time stamp field to integer value
	 *
	 * @param val the integer value
	 * @exception IOException I/O errors
	 */

	public Map setTimeStamp(int val) throws IOException {

		Convert.setIntValue(val, fldOffset[2], data);
		return this;
	}

	/**
	 * Set value field to string value
	 *
	 * @param val the string value
	 * @exception IOException I/O errors
	 */

	public Map setValue(String val) throws IOException {

		Convert.setStrValue(val, fldOffset[3], data);
		return this;
	}

	/**
	 * setHdr will set the header of this map.
	 * 
	 * @exception IOException             I/O errors
	 * @exception InvalidTypeException    Invalid tupe type
	 * @exception InvalidMapSizeException Map size too big
	 *
	 */

	public void setHdr() throws IOException, InvalidTypeException {

		fldCnt = 4;
		Convert.setShortValue((short) 4, map_offset, data);
		fldOffset = new short[4 + 1];
		int pos = map_offset + 2; // start position for fldOffset[]

		fldOffset[0] = (short) ((4 + 2) * 2 + map_offset);

		Convert.setShortValue(fldOffset[0], pos, data);

		int maxLengthOfString = 20;
		fldOffset[0] = (short) (12 + map_offset);
		fldOffset[1] = (short) (fldOffset[0] + maxLengthOfString + 2);
		fldOffset[2] = (short) (fldOffset[1] + maxLengthOfString + 2);
		fldOffset[3] = (short) (fldOffset[2] + 4);
		fldOffset[4] = (short) (fldOffset[3] + maxLengthOfString + 2);

		try {
			Convert.setShortValue(fldCnt, 0, data);
			Convert.setShortValue(fldOffset[0], 2, data);
			Convert.setShortValue(fldOffset[1], 4, data);
			Convert.setShortValue(fldOffset[2], 6, data);
			Convert.setShortValue(fldOffset[3], 8, data);
			Convert.setShortValue(fldOffset[4], 10, data);
		} catch (Exception e) {
		}

		map_length = fldOffset[(short) 4] - map_offset;
	}

	/**
	 * Returns number of fields in this map
	 *
	 * @return the number of fields in this map
	 *
	 */

	public short noOfFlds() {
		return fldCnt;
	}

	/**
	 * Makes a copy of the fldOffset array
	 *
	 * @return a copy of the fldOffset array
	 *
	 */

	public short[] copyFldOffset() {
		short[] newFldOffset = new short[fldCnt + 1];
		for (int i = 0; i <= fldCnt; i++) {
			newFldOffset[i] = fldOffset[i];
		}

		return newFldOffset;
	}

	/**
	 * Print out the map
	 * 
	 * @Exception IOException I/O exception
	 */

	public void print() throws IOException {

		System.out.print("(");
		String rowLabel = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]);
		System.out.print(rowLabel + " , ");
		String colLabel = Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]);
		System.out.print(colLabel + " , ");
		int timeStamp = Convert.getIntValue(fldOffset[2], data);
		System.out.print(timeStamp + " -> ");
		String value = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
		System.out.print(value);
		System.out.print(")");
	}
}
