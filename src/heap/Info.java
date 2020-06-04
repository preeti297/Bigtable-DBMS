/* File Info.java */

//In the directory page, information of data pages is stored in the form of info

package heap;

import java.io.*;

import global.*;

public class Info implements GlobalConst {

	/**
	 * 
	 * Maximum size of any info
	 * 
	 */

	public static final int max_size = MINIBASE_PAGESIZE;

	/**
	 * 
	 * a byte array to hold data
	 * 
	 */

	private byte[] data;

	/**
	 * 
	 * start position of this info in data[]
	 * 
	 */

	private int info_offset;

	/**
	 * 
	 * length of this info
	 * 
	 */

	private int info_length;

	/**
	 * 
	 * private field Number of fields in this info
	 * 
	 * Info has 3 fixed fields, they are availSpace, record count and page id of
	 * data page
	 * 
	 */

	private final short fldCnt = 3;

	/**
	 * 
	 * private field Array of offsets of the fields
	 * 
	 */

	private short[] fldOffset;

	/**
	 * 
	 * Class constructor Create a new info with length = max_size,info offset = 0.
	 * 
	 */

	public Info() {

		data = new byte[max_size];

		info_offset = 0;

		info_length = max_size;

	}

	/**
	 * 
	 * Constructor
	 *
	 * 
	 * 
	 * @param ainfo  a byte array which contains the info
	 * 
	 * @param offset the offset of the info in the byte array
	 * 
	 * @param length the length of the info
	 * 
	 */

	public Info(byte[] ainfo, int offset, int length) {

		data = ainfo;

		info_offset = offset;

		info_length = length;

	}

	/**
	 * 
	 * Constructor(used as info copy)
	 *
	 * 
	 * 
	 * @param fromInfo a byte array which contains the info
	 *
	 * 
	 * 
	 */

	public Info(Info fromInfo) {

		data = fromInfo.getInfoByteArray();

		info_length = fromInfo.getLength();

		info_offset = 0;

		fldOffset = fromInfo.copyFldOffset();

	}

	/**
	 * 
	 * Class constructor Create a new info with length = size,info offset = 0.
	 * 
	 */

	public Info(int size) {

		data = new byte[size];

		info_offset = 0;

		info_length = size;

	}

	/**
	 * 
	 * Copy a info to the current info position you must make sure the info
	 * 
	 * lengths must be equal
	 * 
	 * @param fromInfo the info being copied
	 * 
	 */

	public void infoCopy(Info fromInfo) {

		byte[] temparray = fromInfo.getInfoByteArray();

		System.arraycopy(temparray, 0, data, info_offset, info_length);

	}

	/**
	 * 
	 * This is used when you don't want to use the constructor
	 *
	 * 
	 * 
	 * @param ainfo  a byte array which contains the info
	 * 
	 * @param offset the offset of the info in the byte array
	 * 
	 * @param length the length of the info
	 * 
	 */

	public void infoInit(byte[] ainfo, int offset, int length) {

		data = ainfo;

		info_offset = offset;

		info_length = length;

	}

	/**
	 * 
	 * Set a info with the given info length and offset
	 *
	 * 
	 * 
	 * @param record a byte array contains the info
	 * 
	 * @param offset the offset of the info ( =0 by default)
	 * 
	 * @param length the length of the info
	 * 
	 */

	public void infoSet(byte[] record, int offset, int length) {

		System.arraycopy(record, offset, data, 0, length);

		info_offset = 0;

		info_length = length;

	}

	/**
	 * 
	 * get the length of a info, call this method if you did not call setHdr ()
	 * 
	 * before
	 *
	 * 
	 * 
	 * @return length of this info in bytes
	 * 
	 */

	public int getLength() {

		return info_length;

	}

	/**
	 * 
	 * get the length of a info, call this method if you did call setHdr () before
	 * 
	 * @return size of this info in bytes
	 * 
	 */

	public short size() {

		return ((short) (fldOffset[fldCnt] - info_offset));

	}

	/**
	 * 
	 * get the offset of a info
	 *
	 * 
	 * 
	 * @return offset of the info in byte array
	 * 
	 */

	public int getOffset() {

		return info_offset;

	}

	/**
	 * 
	 * Copy the info byte array out
	 * 
	 * @return byte[], a byte array contains the info the length of byte[] = length
	 * 
	 *         of the info
	 * 
	 */

	public byte[] getInfoByteArray() {

		byte[] infocopy = new byte[info_length];

		System.arraycopy(data, info_offset, infocopy, 0, info_length);

		return infocopy;

	}

	/**
	 * 
	 * return the data byte array
	 *
	 * 
	 * 
	 * @return data byte array
	 * 
	 */

	public byte[] returnInfoByteArray() {

		return data;

	}

	/**
	 * 
	 * Convert this field into integer
	 *
	 * 
	 * 
	 * @param fldNo the field number
	 * 
	 * @return the converted integer if success
	 *
	 * 
	 * 
	 * @exception IOException                    I/O errors
	 * 
	 * @exception FieldNumberOutOfBoundException Info field number out of bound
	 * 
	 */

	public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {

		int val;

		if ((fldNo > 0) && (fldNo <= fldCnt)) {

			val = Convert.getIntValue(fldOffset[fldNo - 1], data);

			return val;

		} else

			throw new FieldNumberOutOfBoundException(null, "INFO:INFO_FLDNO_OUT_OF_BOUND");

	}

	/**
	 * 
	 * Set this field to integer value
	 *
	 * 
	 * 
	 * @param fldNo the field number
	 * 
	 * @param val   the integer value
	 * 
	 * @exception IOException                    I/O errors
	 * 
	 * @exception FieldNumberOutOfBoundException Info field number out of bound
	 * 
	 */

	public Info setIntFld(int fldNo, int val) throws IOException, FieldNumberOutOfBoundException {

		if ((fldNo > 0) && (fldNo <= fldCnt)) {

			Convert.setIntValue(val, fldOffset[fldNo - 1], data);

			return this;

		} else

			throw new FieldNumberOutOfBoundException(null, "INFO:INFO_FLDNO_OUT_OF_BOUND");

	}

	/**
	 * 
	 * setHdr will set the header of this info.
	 *
	 * 
	 * 
	 * @param numFlds    number of fields
	 * 
	 * @param types[]    contains the types that will be in this info
	 * 
	 * @param strSizes[] contains the sizes of the string
	 *
	 * 
	 * 
	 * @exception IOException              I/O errors
	 * 
	 * @exception InvalidTypeException     Invalid tupe type
	 * 
	 * @exception InvalidInfoSizeException Info size too big
	 *
	 * 
	 * 
	 */

	public void setHdr(short numFlds, AttrType types[]) // removed short strSizes[] attribute

			throws IOException, InvalidTypeException, InvalidInfoSizeException {

		if ((numFlds + 2) * 2 > max_size)

			throw new InvalidInfoSizeException(null, "INFO: INFO_TOOBIG_ERROR");

//fldCnt = numFlds; --> fixed as 3

		Convert.setShortValue(numFlds, info_offset, data);

		fldOffset = new short[numFlds + 1];

		int pos = info_offset + 2; // start position for fldOffset[]

// sizeof short =2 +2: array siaze = numFlds +1 (0 - numFilds) and

// another 1 for fldCnt

		fldOffset[0] = (short) ((numFlds + 2) * 2 + info_offset);

		Convert.setShortValue(fldOffset[0], pos, data);

		pos += 2;

		short incr;

		int i;

		for (i = 1; i < numFlds; i++) {

			switch (types[i - 1].attrType) {

			case AttrType.attrInteger:

				incr = 4;

				break;

			default:

				throw new InvalidTypeException(null, "INFO: INFO_TYPE_ERROR");

			}

			fldOffset[i] = (short) (fldOffset[i - 1] + incr);

			Convert.setShortValue(fldOffset[i], pos, data);

			pos += 2;

		}

		switch (types[numFlds - 1].attrType) {

		case AttrType.attrInteger:

			incr = 4;

			break;

		default:

			throw new InvalidTypeException(null, "INFO: INFO_TYPE_ERROR");

		}

		fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);

		Convert.setShortValue(fldOffset[numFlds], pos, data);

		info_length = fldOffset[numFlds] - info_offset;

		if (info_length > max_size)

			throw new InvalidInfoSizeException(null, "INFO: INFO_TOOBIG_ERROR");

	}

	/**
	 * 
	 * Returns number of fields in this info
	 * 
	 * @return the number of fields in this info
	 *
	 * 
	 * 
	 */

	public short noOfFlds() {

		return fldCnt;

	}

	/**
	 * 
	 * Makes a copy of the fldOffset array
	 * 
	 * @return a copy of the fldOffset arrray
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
	 * 
	 * Print out the info
	 *
	 * 
	 * 
	 * @param type the types in the info
	 * 
	 * @Exception IOException I/O exception
	 * 
	 */

	public void print(AttrType type[]) throws IOException {

		int i, val;

		System.out.print("[");

		for (i = 0; i < fldCnt - 1; i++) {

			switch (type[i].attrType) {

			case AttrType.attrInteger:

				val = Convert.getIntValue(fldOffset[i], data);

				System.out.print(val);

				break;

			}

			System.out.print(", ");

		}

		switch (type[fldCnt - 1].attrType) {

		case AttrType.attrInteger:

			val = Convert.getIntValue(fldOffset[i], data);

			System.out.print(val);

			break;

		}

		System.out.println("]");

	}

	/**
	 * 
	 * private method Padding must be used when storing different types.
	 * 
	 * @param offset
	 * 
	 * @param type   the type of info
	 * 
	 * @return short info
	 * 
	 */

	private short pad(short offset, AttrType type) {

		return 0;

	}

}