package iterator;

import java.io.IOException;

import bigt.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

public class Maputils {

	/**
	 * This function compares a map with another map in respective field, and
	 * returns:
	 *
	 * 0 if the two are equal, 1 if the map is greater, -1 if the map is smaller,
	 *
	 * @param fldType   the type of the field being compared.
	 * @param t1        one map.
	 * @param t2        another map.
	 * @param t1_fld_no the field numbers in the maps to be compared.
	 * @param t2_fld_no the field numbers in the maps to be compared.
	 * @exception UnknowAttrType    don't know the attribute type
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @return 0 if the two are equal, 1 if the map is greater, -1 if the map is
	 *         smaller,
	 */
	public static int CompareMapWithMap(AttrType fldType, Map m1, Map m2, int map_fld_no)
			throws IOException, UnknowAttrType, MapUtilsException {
		int m1_i, m2_i;
		String m1_s, m2_s;

		switch (map_fld_no) {
		case 1: 
			m1_s = m1.getRowLabel();
			m2_s = m2.getRowLabel();
			if (m1_s.compareTo(m2_s) > 0)
				return 1;
			if (m1_s.compareTo(m2_s) < 0)
				return -1;
			return 0;

		case 2: 
			m1_s = m1.getColumnLabel();
			m2_s = m2.getColumnLabel();
			if (m1_s.compareTo(m2_s) > 0)
				return 1;
			if (m1_s.compareTo(m2_s) < 0)
				return -1;
			return 0;

		case 4:
			m1_s = m1.getValue();
			m2_s = m2.getValue();
			if (m1_s.length() > m2_s.length())
				return 1;

			else if (m1_s.length() < m2_s.length())
				return -1;

			else
				return m1_s.compareTo(m2_s);

		case 3: 
			m1_i = m1.getTimeStamp();
			m2_i = m2.getTimeStamp();
			if (m1_i == m2_i)
				return 0;
			if (m1_i < m2_i)
				return -1;
			if (m1_i > m2_i)
				return 1;

		case 5:
			m1_s = m1.getColumnLabel() + m1.getRowLabel();
			m2_s = m2.getValue();
			if (m1_s.compareTo(m2_s) > 0)
				return 1;
			if (m1_s.compareTo(m2_s) < 0)
				return -1;
			return 0;

		case 6:
			m1_s = m1.getRowLabel() + m1.getValue();
			m2_s = m2.getValue();
			if (m1_s.compareTo(m2_s) > 0)
				return 1;
			if (m1_s.compareTo(m2_s) < 0)
				return -1;
			return 0;

		default:

			throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

		}
	}

	/**
	 * This function compares a map with another map in respective field and
	 * specifically used for sorting, and returns:
	 *
	 * 0 if the two are equal, 1 if the map is greater, -1 if the map is smaller,
	 *
	 * @param fldType   the type of the field being compared.
	 * @param t1        one map.
	 * @param t2        another map.
	 * @param t1_fld_no the field numbers in the maps to be compared.
	 * @param t2_fld_no the field numbers in the maps to be compared.
	 * @exception UnknowAttrType    don't know the attribute type
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @return 0 if the two are equal, 1 if the map is greater, -1 if the map is
	 *         smaller,
	 */
	public static int CompareMapWithMapForSorting(AttrType fldType, Map m1, Map m2, int map_fld_no)
			throws IOException, UnknowAttrType, MapUtilsException {
		switch (map_fld_no) {
		case 1: 

			try {
				int rowCmp = m1.getRowLabel().compareTo(m2.getRowLabel());
				if (rowCmp != 0) {
					return rowCmp;
				}

				int colCmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
				if (colCmp != 0) {
					return colCmp;
				}

				if (m1.getTimeStamp() == m2.getTimeStamp())
					return 0;
				if (m1.getTimeStamp() < m2.getTimeStamp())
					return -1;
				if (m1.getTimeStamp() > m2.getTimeStamp())
					return 1;
			} catch (Exception e) {
				e.printStackTrace();
			}

		case 2:
			try {
				int colCmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
				if (colCmp != 0) {
					return colCmp;
				}

				int rowCmp = m1.getRowLabel().compareTo(m2.getRowLabel());
				if (rowCmp != 0) {
					return rowCmp;
				}

				if (m1.getTimeStamp() == m2.getTimeStamp())
					return 0;
				if (m1.getTimeStamp() < m2.getTimeStamp())
					return -1;
				if (m1.getTimeStamp() > m2.getTimeStamp())
					return 1;

			} catch (Exception e) {
				e.printStackTrace();
			}

		case 3:
			try {

				int rowCmp = m1.getRowLabel().compareTo(m2.getRowLabel());
				if (rowCmp != 0) {
					return rowCmp;
				}

				if (m1.getTimeStamp() == m2.getTimeStamp())
					return 0;
				if (m1.getTimeStamp() < m2.getTimeStamp())
					return -1;
				if (m1.getTimeStamp() > m2.getTimeStamp())
					return 1;

			} catch (Exception e) {
				e.printStackTrace();
			}

		case 4:
			try {

				int colCmp = m1.getColumnLabel().compareTo(m2.getColumnLabel());
				if (colCmp != 0) {
					return colCmp;
				}

				if (m1.getTimeStamp() == m2.getTimeStamp())
					return 0;
				if (m1.getTimeStamp() < m2.getTimeStamp())
					return -1;
				if (m1.getTimeStamp() > m2.getTimeStamp())
					return 1;

			} catch (Exception e) {
				e.printStackTrace();
			}

		case 5:
			try {
				if (m1.getTimeStamp() == m2.getTimeStamp())
					return 0;
				if (m1.getTimeStamp() < m2.getTimeStamp())
					return -1;
				if (m1.getTimeStamp() > m2.getTimeStamp())
					return 1;
			} catch (Exception e) {
				e.printStackTrace();
			}

		default:

			throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
		}
	}

	/**
	 * This function Compares two Map inn all fields
	 * 
	 * @param t1     the first map
	 * @param t2     the secocnd map
	 * @param type[] the field types
	 * @param len    the field numbers
	 * @return 0 if the two are not equal, 1 if the two are equal,
	 * @exception UnknowAttrType    don't know the attribute type
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @throws FieldNumberOutOfBoundException
	 */

	public static boolean Equal(Map m1, Map m2) throws IOException, UnknowAttrType, MapUtilsException {

		return m1.getRowLabel().equals(m2.getRowLabel()) && m1.getColumnLabel().equals(m2.getColumnLabel())
				&& m1.getTimeStamp() == m2.getTimeStamp() && m1.getValue().equals(m2.getValue());
	}

	/**
	 * get the string specified by the field number
	 * 
	 * @param map   the map
	 * @param fidno the field number
	 * @return the content of the field number
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @throws UnknowAttrType
	 */
	public static String Value(Map map, int fldno) throws IOException, MapUtilsException, UnknowAttrType {
		String temp;
		switch (fldno) {
		case 1:
			temp = map.getRowLabel();
			break;
		case 2:
			temp = map.getColumnLabel();
			break;

		case 4:
			temp = map.getValue();
			break;

		default:
			throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

		}
		return temp;
	}

	/**
	 * set up a map in specified field from a map
	 * 
	 * @param value   the map to be set
	 * @param map     the given map
	 * @param fld_no  the field number
	 * @param fldType the map attr type
	 * @exception UnknowAttrType    don't know the attribute type
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @throws FieldNumberOutOfBoundException
	 */
	public static void SetValue(Map value, Map map, int fld_no, AttrType fldType)
			throws IOException, UnknowAttrType, MapUtilsException, FieldNumberOutOfBoundException {

		value.setRowLabel(map.getRowLabel());
		value.setColumnLabel(map.getColumnLabel());
		value.setTimeStamp(map.getTimeStamp());
		value.setValue(map.getValue());

		return;
	}

	/**
	 * set up the Jmap's attrtype, string size,field number for using join
	 * 
	 * @param Jmap         reference to an actual map - no memory has been malloced
	 * @param res_attrs    attributes type of result map
	 * @param in1          array of the attributes of the map (ok)
	 * @param len_in1      num of attributes of in1
	 * @param in2          array of the attributes of the map (ok)
	 * @param len_in2      num of attributes of in2
	 * @param t1_str_sizes shows the length of the string fields in S
	 * @param t2_str_sizes shows the length of the string fields in R
	 * @param proj_list    shows what input fields go where in the output map
	 * @param nOutFlds     number of outer relation fileds
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 */
	public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs, AttrType in1[], int len_in1, AttrType in2[],
			int len_in2, short t1_str_sizes[], short t2_str_sizes[], FldSpec proj_list[], int nOutFlds)
			throws IOException, MapUtilsException {
		short[] sizesT1 = new short[len_in1];
		short[] sizesT2 = new short[len_in2];
		int i, count = 0;

		for (i = 0; i < len_in1; i++)
			if (in1[i].attrType == AttrType.attrString)
				sizesT1[i] = t1_str_sizes[count++];

		for (count = 0, i = 0; i < len_in2; i++)
			if (in2[i].attrType == AttrType.attrString)
				sizesT2[i] = t2_str_sizes[count++];

		int n_strs = 0;
		for (i = 0; i < nOutFlds; i++) {
			if (proj_list[i].relation.key == RelSpec.outer)
				res_attrs[i] = new AttrType(in1[proj_list[i].offset - 1].attrType);
			else if (proj_list[i].relation.key == RelSpec.innerRel)
				res_attrs[i] = new AttrType(in2[proj_list[i].offset - 1].attrType);
		}

		// Now construct the res_str_sizes array.
		for (i = 0; i < nOutFlds; i++) {
			if (proj_list[i].relation.key == RelSpec.outer
					&& in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
				n_strs++;
			else if (proj_list[i].relation.key == RelSpec.innerRel
					&& in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
				n_strs++;
		}

		short[] res_str_sizes = new short[n_strs];
		count = 0;
		for (i = 0; i < nOutFlds; i++) {
			if (proj_list[i].relation.key == RelSpec.outer
					&& in1[proj_list[i].offset - 1].attrType == AttrType.attrString)
				res_str_sizes[count++] = sizesT1[proj_list[i].offset - 1];
			else if (proj_list[i].relation.key == RelSpec.innerRel
					&& in2[proj_list[i].offset - 1].attrType == AttrType.attrString)
				res_str_sizes[count++] = sizesT2[proj_list[i].offset - 1];
		}
		try {
			Jmap.setHdr();
		} catch (Exception e) {
			throw new MapUtilsException(e, "setHdr() failed");
		}
		return res_str_sizes;
	}

	public static int CompareMapWithValue(AttrType fldType, Map t1, int t1_fld_no, Map value)
			throws IOException, UnknowAttrType, MapUtilsException {
		return CompareMapWithMap(fldType, t1, value, t1_fld_no);
	}

	/**
	 * set up the Jmap's attrtype, string size,field number for using project
	 * 
	 * @param Jmap         reference to an actual map - no memory has been malloced
	 * @param res_attrs    attributes type of result map
	 * @param in1          array of the attributes of the map (ok)
	 * @param len_in1      num of attributes of in1
	 * @param t1_str_sizes shows the length of the string fields in S
	 * @param proj_list    shows what input fields go where in the output map
	 * @param nOutFlds     number of outer relation fileds
	 * @exception IOException       some I/O fault
	 * @exception MapUtilsException exception from this class
	 * @exception InvalidRelation   invalid relation
	 */

	public static short[] setup_op_map(Map Jmap, AttrType res_attrs[], AttrType in1[], int len_in1,
			short t1_str_sizes[], FldSpec proj_list[], int nOutFlds)
			throws IOException, MapUtilsException, InvalidRelation {
		short[] sizesT1 = new short[len_in1];
		int i, count = 0;

		for (i = 0; i < len_in1; i++)
			if (in1[i].attrType == AttrType.attrString)
				sizesT1[i] = t1_str_sizes[count++];

		int n_strs = 0;
		/*
		 * for (i = 0; i < nOutFlds; i++) { if (proj_list[i].relation.key ==
		 * RelSpec.outer) res_attrs[i] = new AttrType(in1[proj_list[i].offset -
		 * 1].attrType);
		 * 
		 * else throw new InvalidRelation("Invalid relation -innerRel"); }
		 */

		// Now construct the res_str_sizes array.
		/*
		 * for (i = 0; i < nOutFlds; i++) { if (proj_list[i].relation.key ==
		 * RelSpec.outer && in1[proj_list[i].offset - 1].attrType ==
		 * AttrType.attrString) n_strs++; }
		 */

		short[] res_str_sizes = new short[] { 22, 22, 22 };
		count = 0;
		/*
		 * for (i = 0; i < nOutFlds; i++) { if (proj_list[i].relation.key ==
		 * RelSpec.outer && in1[proj_list[i].offset - 1].attrType ==
		 * AttrType.attrString) res_str_sizes[count++] = sizesT1[proj_list[i].offset -
		 * 1]; }
		 */

		try {
			Jmap.setHdr();
		} catch (Exception e) {
			throw new MapUtilsException(e, "setHdr() failed");
		}
		return res_str_sizes;
	}

}
