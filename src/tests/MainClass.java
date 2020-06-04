package tests;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import bufmgr.BufMgr;
import diskmgr.Pcounter;
import global.SystemDefs;

/**
 * This MainClass allows user to select from two options to perform batch insert
 * and query retrieval
 */

public class MainClass {

	public static void main(String[] args) {

		String dbpath;

		@SuppressWarnings("unused")
		SystemDefs sysDef = null;

		Scanner in = new Scanner(System.in);
		System.out.println("Please enter option:\n\n" + "[1] Batch Insert\n" + "[2] Query Retrieval\n"
				+ "[3] Row Count\n" + "[4] Column Count\n" + "[5] Exit");
		String msg = in.nextLine();
		int input = Integer.valueOf(msg);
		int numbuf = 0;

		try {

			while (input != 5) 
			{
				
				switch (input) 
					{
	
					case 1:
						System.out.println("enter parameters for Batch Insert\n\n "
								+ "[1] batchinsert\n\n "
								+ "[2] csv file along with it's path from where you want to insert \n\n "
								+ "[3] Type of indexing\n\n " + "		1 - >  No index\n "
								+ "		2 - >  Row label index \n " + "		3 - >  column label index \n "
								+ "		4 - >  1st index on column and row combined; 2nd index on timestamp \n "
								+ "		5 - >  1st index on row and value combined; 2nd index on timestamp \n\n "
								+ "[4] name of the big table you want to insert data into\n\n "
								+ "[5] number of buffers you want to use for your insertion\n\n");
						Pcounter.initialize();
						msg = in.nextLine();
						String[] str = msg.split(" ");
						args = str;
						String dataFile =  args[1]; /// Users/sruthi/Downloads/
						// dont remove this --> /Users/saiuttej/Documents/DBMSI/project material/
						int type = Integer.parseInt(args[2]);
						String bigTableName = args[3];
						numbuf = Integer.parseInt(args[4]);
						BatchInsert btch = new BatchInsert();
						dbpath = "/tmp/" + bigTableName + String.valueOf(type) + System.getProperty("user.name")
								+ ".minibase-db";
						if (sysDef == null || !SystemDefs.JavabaseDB.db_name().equals(dbpath)) {
							if (sysDef != null)
								SystemDefs.JavabaseDB.DBDestroy();
							sysDef = new SystemDefs(dbpath, 100000, numbuf, "Clock", type);
							SystemDefs.JavabaseDB._initDB(bigTableName + String.valueOf(type));
						} else {
							SystemDefs.JavabaseBM.flushAll();
							SystemDefs.JavabaseBM = new BufMgr(numbuf, "Clock");
							SystemDefs.JavabaseDB.destroyIndex();
						}
	
						long startTime = System.nanoTime();
						btch.insert(SystemDefs.JavabaseDB, dataFile, numbuf);
						long endTime = System.nanoTime();
						System.out.println("Time Taken for batch insert operation is " + ((endTime - startTime)/1000000000) + " s");
						System.out.println("no, of pages written are   " + Pcounter.wcounter);
						System.out.println("no, of pages read are  " + Pcounter.rcounter);
						
						break;
	
					case 2: {
						System.out.println("enter parameters for query\n\n"
								+ "[1] query\n\n"
								+ "[2] big table where you have your records\n\n"
								+ "[3] index type using which you inserted records into your big table\n\n"
								+ "[4] orderType - the way you want your records to be ordered\n\n"
								+ "		1 - > row label first, then column label, then time stamp\n"
								+ "		2 - > column label first , then row label, then time stamp\n"
								+ "		3 - > row label first, then time stamp\n"
								+ "		4 - > column label first, then time stamp\n" + "		5 - > only on timestamp\n\n"
								+ "[5] row filter (*, a single value or a range in the format [x,y]) \n\n"
								+ "[6] column filter (*, a single value or a range in the format [x,y]) \n\n"
								+ "[7] value filter (*, a single value or a range in the format [x,y]) \n\n"
								+ "[8] Number of buffers you want to use for your query(should be less than or equal to those given for batch insertion)\n\n");
	
						Pcounter.initialize();
						msg = in.nextLine();
						String[] str1 = msg.split(" ");
						args = str1;
						int queryBuf = Integer.parseInt(args[7]);
						SystemDefs.JavabaseBM.flushAll();
						SystemDefs.JavabaseBM = new BufMgr(numbuf, "Clock");
						Query query = new Query();
						
						startTime = System.nanoTime();
					
						query.retrieve(SystemDefs.JavabaseDB, Integer.parseInt(args[3]), args[4], args[5], args[6],
								queryBuf);
					
						endTime = System.nanoTime();
						System.out.println("Time Taken for Query Retrieval operation is " + ((endTime - startTime)/1000000000) + " s");
						System.out.println("no, of pages written are   " + Pcounter.wcounter);
						System.out.println("no, of pages read are  " + Pcounter.rcounter);
						
						
						break;
					}
	
					case 3: {
						System.out.println("enter number of buffers\n");
						msg = in.nextLine();
						String[] str1 = msg.split(" ");
						args = str1;
						numbuf = Integer.parseInt(args[0]);
						SystemDefs.JavabaseDB.getRowCnt(numbuf);
						break;
					}
	
					case 4: {
						System.out.println("enter number of buffers \n");
						msg = in.nextLine();
						String[] str1 = msg.split(" ");
						args = str1;
						numbuf = Integer.parseInt(args[0]);
						SystemDefs.JavabaseDB.getColumnCnt(numbuf);
						break;
					}
					case 5: {
						break;
					}
	
					}
				System.out.println("Please enter option:\n\n" + "[1] Batch Insert\n" + "[2] Query Retrieval\n"
						+ "[3] Row Count\n" + "[4] Column Count\n" + "[5] Exit");
				msg = in.nextLine();
				input = Integer.valueOf(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
