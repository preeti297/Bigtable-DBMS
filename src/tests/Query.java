package tests;

import java.io.IOException;

import bigt.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import diskmgr.BigDB;
import diskmgr.Pcounter;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidTypeException;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.SortException;
import iterator.TupleUtilsException;

/**
 * Query is called from MainClass when user selects option 2.
 * 
 * Here, the big table is queried based on the parameters given
 */

public class Query {

	Query() {

	}

	/**
	 * Initializes a stream according to orderType given and gets records from
	 * Stream class using getNext functionality
	 * 
	 * @param bigdb     - the Big Table from which you are retrieving the records
	 * @param orderType - the order in which you want the records to be retrieved
	 * @param rowFilter - filter for rows ( *, a single value or a range in the form
	 *                  [x,y] where x,y are 'row' values in a map )
	 * @param colFilter - filter for columns ( *, a single value or a range in the
	 *                  form [x,y] where x,y are 'column' values in a map)
	 * @param valFilter - filter for values ( *, a single value or a range in the
	 *                  form [x,y] where x,y are 'value' values in a map )
	 * @param numbuf    - number of buffers used to retrieve records
	 */

	public void retrieve(BigDB bigdb, int orderType, String rowFilter, String colFilter, String valFilter, int numbuf) {
		try {
			Stream res = bigdb.openStreamDB(orderType, rowFilter, colFilter, valFilter, numbuf);
			System.out.println("\n\nThe matching records are\n\n");
			Map t = null;
			t = res.getNext();

			while (t != null) {
				t.print();
				System.out.println("");
				t = res.getNext();
			}

			
			res.closestream();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// print disk read and write
	}

}
