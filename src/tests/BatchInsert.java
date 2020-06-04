package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

//import array for bigDB
import bigt.Map;
import diskmgr.BigDB;
import diskmgr.Pcounter;

/**
 * BatchInsert is called from MainClass when user selects option 1.
 * 
 * Here, batch insert happens from csv file given by the user to a big table
 */

public class BatchInsert extends TestDriver {

	String[] args = new String[6];

	BatchInsert() {

	}

	/**
	 * to insert records into Big Table
	 * 
	 * @param bigdb    - the Big Table into which you want to insert records(given
	 *                 by user)
	 * @param dataFile - the data file from where we extract records to insert into
	 *                 bigdb
	 * @param numbuf   - number of buffers allocated for batch insert
	 *
	 */
	public void insert(BigDB bigdb, String dataFile, int numbuf) {

		Map map = null;
		try {

			BufferedReader bin = new BufferedReader(new FileReader(dataFile));
			String line = "";
			while ((line = bin.readLine()) != null) {
				// Remove all the garbage values
				line = line.replaceAll("[^\\x00-\\x7F]", "");

				map = new Map();
				map.setHdr();

				String[] tokens = line.trim().split(",");

				String rowLabel = tokens[0];
				String colLabel = tokens[1];
				String value = tokens[2];
				Integer timeStamp = Integer.parseInt(tokens[3]);

				// Populate the values to a map
				map.setRowLabel(rowLabel);
				map.setColumnLabel(colLabel);
				map.setTimeStamp(timeStamp);
				map.setValue(value);

				// Insert the map into bigdb
				bigdb.insert(map.getMapByteArray());

			}
			bigdb.populatedb();
			bin.close();

			bigdb.removeDuplicates();

			bigdb.insertindex();


			System.out.println("Map count is " + bigdb.getMapCount());

		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
