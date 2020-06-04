package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import chainexception.*;
import bigt.Map;
import bigt.BigT;

/**
 * Note that in JAVA, methods can't be overridden to be more private. Therefore,
 * the declaration of all private functions are now declared protected as
 * opposed to the private type in C++.
 */

class HFDriver extends TestDriver implements GlobalConst {

	private final static boolean OK = true;
	private final static boolean FAIL = false;

	private int choice;
	private final static int reclen = 82;

	public HFDriver() {
		super("hptest");
		choice = 10; // big enough for file to occupy > 1 data page
		// choice = 2000; // big enough for file to occupy > 1 directory page
		// choice = 5;
	}

	public boolean runTests() {

		System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

		SystemDefs sysdef = new SystemDefs(dbpath, 100, 100, "Clock");

		// Kill anything that might be hanging around
		String newdbpath;
		String newlogpath;
		String remove_logcmd;
		String remove_dbcmd;
		String remove_cmd = "/bin/rm -rf ";

		newdbpath = dbpath;
		newlogpath = logpath;

		remove_logcmd = remove_cmd + logpath;
		remove_dbcmd = remove_cmd + dbpath;

		// Commands here is very machine dependent. We assume
		// user are on UNIX system here
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		remove_logcmd = remove_cmd + newlogpath;
		remove_dbcmd = remove_cmd + newdbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		// Run the tests. Return type different from C++
		boolean _pass = runAllTests();

		// Clean up again
		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
		} catch (IOException e) {
			System.err.println("IO error: " + e);
		}

		System.out.print("\n" + "..." + testName() + " tests ");
		System.out.print(_pass == OK ? "completely successfully" : "failed");
		System.out.print(".\n\n");

		return _pass;
	}

	protected boolean test1() {

		System.out.println("\n  Test 1: Insert and scan fixed-size records\n");
		boolean status = OK;
		MID rid = new MID();
		BigT f = null;

		System.out.println("  - Create a BigT\n");
		try {
			f = new BigT("file_1", 1);

		} catch (Exception e) {
			status = FAIL;
			System.err.println("*** Could not create heap file\n");
			e.printStackTrace();
		}

		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
			System.err.println("*** The heap file has left pages pinned\n");
			status = FAIL;
		}

		if (status == OK) {
			System.out.println("  - Add " + choice + " records to the file\n");
			for (int i = 0; (i < choice) && (status == OK); i++) {

				// fixed length record
				Map rec = new Map(reclen);

				try {
					rec.setHdr();
					rec.setRowLabel("row" + i);
					rec.setColumnLabel("col" + i);
					rec.setTimeStamp(i);
					rec.setValue("val" + i);
					rid = f.insertMap(rec.getMapByteArray());
				} catch (Exception e) {
					status = FAIL;
					System.err.println("*** Error inserting record " + i + "\n");
					e.printStackTrace();
				}

				if (status == OK
						&& SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {

					System.err.println("*** Insertion left a page pinned\n");
					status = FAIL;
				}
			}

			try {
				if (f.getMapCnt() != choice) {
					status = FAIL;
					System.err.println("*** File reports " + f.getMapCnt() + " records, not " + choice + "\n");
				}
			} catch (Exception e) {
				status = FAIL;
				System.out.println("" + e);
				e.printStackTrace();
			}


		if (status == OK)
			System.out.println("  Test 1 completed successfully.\n");
		}
		return status;
	}
		

	protected boolean test2() {

		System.out.println("\n  Test 2: Delete fixed-size records\n");
		boolean status = OK;
		Scan scan = null;
		MID rid = new MID();
		Heapfile f = null;

		System.out.println("  - Open the same heap file as test 1\n");
		try {
			f = new Heapfile("file_1");
		} catch (Exception e) {
			status = FAIL;
			System.err.println(" Could not open heapfile");
			e.printStackTrace();
		}

		if (status == OK) {
			System.out.println("  - Delete half the records\n");
			try {
				scan = f.openScan();
			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		if (status == OK) {
			int i = 0;
			Map map = new Map();
			boolean done = false;

			while (!done) {
				try {
					map = scan.getNext(rid);
					if (map == null) {
						done = true;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					boolean odd = true;
					if (i % 2 == 1)
						odd = true;
					if (i % 2 == 0)
						odd = false;
					if (odd) { // Delete the odd-numbered ones.
						try {
							status = f.deleteRecord(rid);
						} catch (Exception e) {
							status = FAIL;
							System.err.println("*** Error deleting record " + i + "\n");
							e.printStackTrace();
							break;
						}
					}
				}
				++i;
			}
		}

		scan.closescan(); // Close scan
		scan = null;

		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {

			System.out.println(
					"\nt2: in if: Number of unpinned buffers: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
			System.err.println("t2: in if: getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

			System.err.println("*** Deletion left a page pinned\n");
			status = FAIL;
		}

		if (status == OK) {
			System.out.println("  - Scan the remaining records\n");
			try {
				scan = f.openScan();
			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		if (status == OK) {
			int len, i = 0;
			Map rec = null;
			Map map = new Map();
			boolean done = false;

			while (!done) {
				try {
					map = scan.getNext(rid);
					if (map == null) {
						done = true;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					try {
						rec = new Map(map);
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}

					try {
						if ((rec.getTimeStamp() != i) || (!rec.getRowLabel().equals("row" + i))
								|| (!rec.getColumnLabel().equals("col" + i))) {
							System.err.println("*** Record " + i + " differs from what we inserted\n");
							System.err.println("rec.timestamp: " + rec.getTimeStamp() + " should be " + i + "\n");
							System.err.println("rec.getRowLabel: " + rec.getRowLabel() + " should be " + (i * 2.5) + "\n");
							System.err.println("rec.getColumnLabel: " + rec.getColumnLabel());

							status = FAIL;
							break;
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					i += 2; // Because we deleted the odd ones...
				}
			}
		}

		if (status == OK)
			System.out.println("  Test 2 completed successfully.\n");
		return status;

	}

	protected boolean test3() {

		System.out.println("\n  Test 3: Update fixed-size records\n");
		boolean status = OK;
		Scan scan = null;
		MID rid = new MID();
		Heapfile f = null;

		System.out.println("  - Open the same heap file as tests 1 and 2\n");
		try {
			f = new Heapfile("file_1");
		} catch (Exception e) {
			status = FAIL;
			System.err.println("*** Could not create heap file\n");
			e.printStackTrace();
		}

		if (status == OK) {
			System.out.println("  - Change the records\n");
			try {
				scan = f.openScan();
			} catch (Exception e) {
				status = FAIL;
				System.err.println("*** Error opening scan\n");
				e.printStackTrace();
			}
		}

		if (status == OK) {

			int len, i = 0;
			Map rec = null;
			Map map = new Map();
			boolean done = false;

			while (!done) {
				try {
					map = scan.getNext(rid);
					if (map == null) {
						done = true;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					try {
						rec = new Map(map);
						rec.setTimeStamp(7 * i);
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}

					// rec.fval =(float) 7*i; // We'll check that i==rec.ival below.

					Map newMap = null;
					try {
						newMap = new Map(rec.getMapByteArray(), 0);
						newMap.setHdr();

					} catch (Exception e) {
						status = FAIL;
						System.err.println("" + e);
						e.printStackTrace();
					}
					try {
						status = f.updateRecord(rid, newMap);
					} catch (Exception e) {
						status = FAIL;
						e.printStackTrace();
					}

					if (status != OK) {
						System.err.println("*** Error updating record " + i + "\n");
						break;
					}
					i += 2; // Recall, we deleted every other record above.
				}
			}
		}

		scan = null;

		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {

			System.out.println("t3, Number of unpinned buffers: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "\n");
			System.err.println("t3, getNumbfrs: " + SystemDefs.JavabaseBM.getNumBuffers() + "\n");

			System.err.println("*** Updating left pages pinned\n");
			status = FAIL;
		}

		if (status == OK) {
			System.out.println("  - Check that the updates are really there\n");
			try {
				scan = f.openScan();
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			if (status == FAIL) {
				System.err.println("*** Error opening scan\n");
			}
		}

		if (status == OK) {
			int len, i = 0;
			Map rec = null;
			Map rec2 = null;
			Map map = new Map();
			Map map2 = new Map();
			boolean done = false;

			while (!done) {
				try {
					map = scan.getNext(rid);
					if (map == null) {
						done = true;
						break;
					}
				} catch (Exception e) {
					status = FAIL;
					e.printStackTrace();
				}

				if (!done && status == OK) {
					try {
						rec = new Map(map);
						rec.setHdr();
					} catch (Exception e) {
						System.err.println("" + e);
					}

					// While we're at it, test the getRecord method too.
					try {
						map2 = f.getRecord(rid);
					} catch (Exception e) {
						status = FAIL;
						System.err.println("*** Error getting record " + i + "\n");
						e.printStackTrace();
						break;
					}

					try {
						rec2 = new Map(map2);
						rec2.setHdr();
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}
					try {

						if (rec.getTimeStamp() != 7 * i) {
							System.err.println("*** Record " + i + " differs from our update\n");
							System.err.println("rec.ival: " + rec.getTimeStamp() + " should be " + i + "\n");

							status = FAIL;
							break;
						}
					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
					}

				}
				i += 2; // Because we deleted the odd ones...
			}
		}

		if (status == OK)
			System.out.println("  Test 3 completed successfully.\n");
		return status;

	}

	

	protected boolean runAllTests() {

		boolean _passAll = OK;

		if (!test1()) {
			_passAll = FAIL;
		}
		if (!test2()) {
			_passAll = FAIL;
		}
		if (!test3()) {
			_passAll = FAIL;
		}
		return _passAll;
	}

	protected String testName() {

		return "Heap File";
	}
}


public class HFTest {

	public static void main(String argv[]) {

		HFDriver hd = new HFDriver();
		boolean dbstatus;

		dbstatus = hd.runTests();

		if (dbstatus != true) {
			System.err.println("Error encountered during buffer manager tests:\n");
			Runtime.getRuntime().exit(1);
		}

		Runtime.getRuntime().exit(0);
	}
}
