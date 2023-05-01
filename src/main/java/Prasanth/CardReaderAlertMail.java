package Prasanth;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.BasicBSONObject;
import org.bson.Document;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public class CardReaderAlertMail {

	static MongoClient mongoClient = null;
	static MongoClient mongo = null;
	static MongoCredential credential = null;
	static DBCollection summaryColl = null;
	static DBCollection orgColl = null;
	static DB db1 = null;
	static DB db2 = null;
	static String ip_address = null;
	static int port_no = 0;
	static InputStream property_reader = null;
	static Properties properties = null;
	static Calendar calendar = null;
	static int duration = 0;
	static XSSFWorkbook group = new XSSFWorkbook();
	static ArrayList<XSSFWorkbook> grouplist = new ArrayList<XSSFWorkbook>();
	static OutputStream out = null;
	static String event_id[] = null;
	static List<Integer> eventId = new ArrayList<Integer>();

	public static void main(String[] args) throws IOException {
		try {
			// Reading property file
			property_reader = new FileInputStream("CardReaderAlertMail.properties");
			properties = new Properties();
			properties.load(property_reader);
			ip_address = properties.getProperty("ip_address");
			port_no = Integer.valueOf(properties.getProperty("port_no"));
			duration = Integer.valueOf(properties.getProperty("duration"));
			event_id = properties.getProperty("event_config").split(",");
			for (int i = 0; i < event_id.length; i++) {
				eventId.add(Integer.parseInt(event_id[i]));
			}
			System.out.println("Server connected with " + ip_address);
			try {
				Builder builder = MongoClientOptions.builder().serverSelectionTimeout(2000);
				mongo = new MongoClient(new ServerAddress(ip_address, port_no), builder.build());
				mongo.getAddress();
				try {
					System.out.println("Connection verified !!!");

					credential = MongoCredential.createCredential("admin", "admin", "admin".toCharArray());
					mongoClient = new MongoClient(new ServerAddress(ip_address, port_no), Arrays.asList(credential));
					System.out.println("Connection establilished !!!");

					db1 = mongoClient.getDB("interface");
					System.out.println("Connected DB is " + db1.getName());
					db2 = mongoClient.getDB("snoc");
					System.out.println("Connected DB is " + db2.getName());
					summaryColl = db1.getCollection("transaction_summary");
					orgColl = db1.getCollection("organization");
					System.out.println(summaryColl.getName() + " collection connected");
					System.out.println(orgColl.getName() + " collection connected");
					executeQuery();
				} catch (MongoException e) {
					mongoClient.close();
				}

			} catch (MongoException e) {
				System.out.println("Mongo is down!! or Invalid access");
			}
		} catch (FileNotFoundException e) {
			System.out.println("Property file error !!");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Property file location error !!");
			e.printStackTrace();
		} finally {
			System.out.println("Jar operation end!! ");
			mongo.close();
		}
	}

	public static Date atStartOfDay(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}

	public static Date atEndOfDay(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 23);
		calendar.set(Calendar.MINUTE, 59);
		calendar.set(Calendar.SECOND, 59);
		calendar.set(Calendar.MILLISECOND, 999);
		return calendar.getTime();
	}

	static void executeQuery() throws IOException {

		calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -duration);
		Calendar calendar_today = Calendar.getInstance();
		calendar_today.add(Calendar.DATE, -1);
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
		long start_dt_num = Long.valueOf(sdf1.format(atStartOfDay(calendar.getTime())));
		long end_dt_num = Long.valueOf(sdf1.format(atEndOfDay(calendar_today.getTime())));
		System.out.println("Query with start date " + start_dt_num);
		System.out.println("Query with end date " + end_dt_num);

		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet sheet = workbook.createSheet("CardReaderReport");

		// Aggregation query builder

		// Matching condition

		List<Document> match_param = Arrays.asList(new Document("$match",
				new Document("crtd_dt_num", new Document("$gt", start_dt_num).append("$lt", end_dt_num))
						.append("event_id", new Document("$in", eventId))));
		BasicDBObject match = new BasicDBObject(match_param.get(0));

		// Grouping based on the event id and event name
		List<Document> grp_param = Arrays
				.asList(new Document("orgid", "$orgid")
						.append("crtd_dt",
								new Document("$dateToString",
										new Document("format", "%Y-%m-%d").append("date", "$crtd_dt")))
						.append("eventName", "$event_name"));

		// Taking sum count
		BasicDBObject sum = new BasicDBObject("$sum", 1);

		// Giving the condition
		List<Document> yesCount_param = Arrays.asList(new Document("$sum", new Document("$cond",
				new Document("if", new Document("$eq", Arrays.asList("$interface.data_via_card_reader", "true")))
						.append("then", 1).append("else", 0))));

		// Putting into the group
		BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", grp_param.get(0))
				.append("count", sum).append("yesCount", yesCount_param.get(0)));

		List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
		pipeline.add(match);
		pipeline.add(group);

		// Executing the Query
		@SuppressWarnings("deprecation")
		AggregationOutput result = summaryColl.aggregate(pipeline); // transaction summary
		Iterator itr = result.results().iterator();

		Map<Integer, Object[]> data = new TreeMap<Integer, Object[]>();
		int i = 1;
		data.put(i, new Object[] { "TransactionDate", "OrganizationCode", "EventName", "UsingCardReaderCount",
				"NotUsingCardReaderCount", "TotalCount" });

		while (itr.hasNext()) {
			BasicDBObject dbo = (BasicDBObject) itr.next();
			BasicBSONObject b = (BasicBSONObject) dbo.get("_id");
			int count = (Integer) dbo.get("count");
			int used_count = dbo.getInt("yesCount");
			int not_used_count = count - used_count;
			String eventName = b.getString("eventName");
			int org_id = b.getInt("orgid");
			BasicDBObject query = new BasicDBObject();
			query.put("_id", org_id);
			DBCursor cursor = orgColl.find(query);
			String org_code = null;
			while (cursor.hasNext()) {
				BasicDBObject bdo = (BasicDBObject) cursor.next();
				org_code = bdo.getString("ref_code");
			}
			String date = b.getString("crtd_dt");
			i++;
			data.put(i, new Object[] { date, org_code, eventName, used_count, not_used_count, count });
		}
		System.out.println("Query operation completed");
		Set<Integer> keyset = data.keySet();

		int rownum = 0;

		for (Integer key : keyset) {
			Row row = sheet.createRow(rownum++);
			Object[] objArr = data.get(key);

			int cellnum = 0;
			for (Object obj : objArr) {
				Cell cell = row.createCell(cellnum++);

				if (obj instanceof String) {
					cell.setCellValue((String) obj);
				}

				else if (obj instanceof Integer) {
					cell.setCellValue((Integer) obj);
				}

			}
		}
		try {
			FileOutputStream outputstream = new FileOutputStream(new File("CardReaderOutputCumulative.xlsx"));
			workbook.write(outputstream);
			System.out.println("Hey, Data have been successfully imported to the Excel file");
		} catch (Exception e) {
			System.out.println("Error with the data exportation");
			e.printStackTrace();
		}
		System.out
				.println("The output file is about to take for grouping process");

		FileInputStream inputstream = new FileInputStream("CardReaderOutputCumulative.xlsx");
		XSSFWorkbook workbook_read = new XSSFWorkbook(inputstream);

		XSSFSheet sheet1 = workbook_read.getSheetAt(0); // only one sheet is in the file so i took directly and stored

		workbook_read.close();
		Iterator<Row> rowIterator = sheet1.iterator();
		if (rowIterator.hasNext()) {
			Row header = rowIterator.next(); // storing the header data and ignoring header

			while (rowIterator.hasNext()) {
				Row row = rowIterator.next(); // Iterating each row of the excel sheet excluding the header
				Cell cell = row.getCell(2); // Taking particular column data of the each row. Here 2 is denoting event
											// name
				String event_name = cell.getStringCellValue();
				int number = validateGroup(header, event_name); // Calling grouping method and getting the array list
																// size
				XSSFWorkbook group1 = grouplist.get(number);
				XSSFSheet sh = group1.getSheet(event_name);
				int rc = sh.getLastRowNum() + 1;

				// creating a row
				XSSFRow rowx = sh.createRow(rc);
				for (Cell c : row) {
					// Creating cells
					CellType cellType = c.getCellType();
					XSSFCell cc = rowx.createCell(c.getColumnIndex(), cellType);

					switch (cellType) {
					case STRING:
						cc.setCellValue(c.getStringCellValue());
						break;
					case NUMERIC:
						cc.setCellValue(c.getNumericCellValue());
						break;
					default:
						break;
					}
				}
			}
			// Grouped workbooks are about to iterate below

			for (XSSFWorkbook book : grouplist) {
				String name = book.getSheetAt(0).getSheetName();
				out = new FileOutputStream("CardReaderOutputCumulative.xlsx");
				book.write(out);
			}
			out.close();
			System.out.println("Grouping the excel sheet process completed !!!.");
		} else {
			System.out.println("Source file is empty");
		}
	}

	public static int validateGroup(Row header, String value)  {

		// This is the grouping method.

		for (int i = 0; i < grouplist.size(); i++) { // here i am checking that sheet is already present or not
			group = grouplist.get(i);
			XSSFSheet sheet = group.getSheet(value);
			if (sheet != null) { // If is not satisfied then new sheet would be created and its index would be
									// 1++
				return i; // it will skip the below code if there is group already
			}
		}
		// create a new sheet
		XSSFSheet sheet = group.createSheet(value); // I commonly created the workbook where i am creating the new
													// sheets
		XSSFRow row = sheet.createRow(0);
		int j = 0;
		Iterator<Cell> iterator = header.iterator();
		while (iterator.hasNext()) { // Logic to insert the header into the sheet
			j++;
			Cell cell = iterator.next();
			XSSFCell c = row.createCell(j);
			c.setCellValue(cell.getStringCellValue());
		}
		grouplist.add(group); // Adding the created workbook into the array of workbook list
		return grouplist.size() - 1;
	}

}
