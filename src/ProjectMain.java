import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * This program creates a movie review database called "iRate". There are 
 * entity tables for the tables customer, movie, attendance, review,
 * and endorsement. This program uses the ER data model.
 * 
 * @author b garbo & michelle lee 
 *
 */

public class ProjectMain {
	
	public static Connection connection;
	
	public static Connection getConnection() {
		return connection;
	}

	public static void main(String[] args) {
	
		// embed default framework
		String protocol = "jdbc:derby:";
		String dbName = "irate";
		String connStr = protocol + dbName + ";create=true";
		
		// tables created
		String dbTables[] = {
				"Customer", "Movie", "Attendance", "Review", "Endorsement"
		};
		
		// functions created 
		String dbFunctions[] = {
				"generateUuid", "sevenDays", "threeDays", "oneDay",
				"isValidEndorsement", "verifyEndorsement", "isUuid",
				"verifyAttendance", "isOnlyReview"
		};
		
		// types created
		String dbTypes[] = {
				"uuid"
		};
		
		Properties props = new Properties(); // connection properties
		props.put("user", "user1");
		props.put("password", "user1");
		
		
		try (
			// connection
			Connection conn = DriverManager.getConnection(connStr, props);
			
			// statement
			Statement stmt = conn.createStatement();
		) {
			System.out.println("Connected to and created database " + dbName);
			connection = conn;
			
			// drop tables
			for (String tbl : dbTables) {
				try { 
					stmt.executeUpdate("drop table " + tbl);
					System.out.println("Dropped table " + tbl);
				} catch (SQLException ex) {
					System.out.println("Did not drop table " + tbl);
				}
			}
			
			// drop functions
			for (String func : dbFunctions) {
				try {
					stmt.executeUpdate("drop function " + func);
					System.out.println("Dropped function " + func);
				} catch (SQLException ex) {
					System.out.println("Did not drop function " + func);
				}
			}
			
			// drop types 
			for (String type : dbTypes) {
				try { 
					stmt.executeUpdate("drop type " + type + " restrict");
					System.out.println("Dropped type " + type);
				} catch (SQLException ex) {
					System.out.println("Did not drop type " + type);
				}
			}
			
			// create UUID type - unused, but allows for storing UUID 
			// as new type within the database (vs varchar) 
			String createType_Uuid = 
					" create type uuid" +
					"  external name " +
					"     'java.util.UUID'" +
					"  language java";
			stmt.executeUpdate(createType_Uuid);
			System.out.println("Created type UUID");
			
			
			// STORED FUNCTIONS FROM Storedfunctions.Java
			
			// check seven days function
			String createFunction_sevenDays = 
					"create function sevenDays(" +
					"  storedDate date," + 
					"  oldDate date" +
					") returns boolean " +
					" language java " + 
					" parameter style java " + 
					" external name 'StoredFunction.sevenDays'";
			stmt.executeUpdate(createFunction_sevenDays);
			System.out.println("Created function sevenDays");
			
			// check seven days function
			String createFunction_threeDays = 
					"create function threeDays(" +
					"  storedDate date," + 
					"  oldDate date" +
					") returns boolean " +
					" language java " + 
					" parameter style java " + 
					" external name 'StoredFunction.threeDays'";
			stmt.executeUpdate(createFunction_threeDays);
			System.out.println("Created function threeDays");
			
			// check seven days function
			String createFunction_oneDay = 
					"create function oneDay(" +
					"  storedDate date," + 
					"  oldDate date" +
					") returns boolean " +
					" language java " + 
					" parameter style java " + 
					" external name 'StoredFunction.oneDay'";
			stmt.executeUpdate(createFunction_oneDay);
			System.out.println("Created function oneDay");
			
			// create UUID function - could be used to generate a new uuid
			// at the time of insert, though is not implemented for this test
			String createFunction_generateUuid = 
					"create function generateUuid()" +
					"  returns varchar(36)" + 
					"  language java" +
					"  parameter style java" +
					"  external name " +
					"     'java.util.UUID.randomUUID.toString'";
			stmt.executeUpdate(createFunction_generateUuid);
			System.out.println("Created function generateUuid");
			
			// create check validEndorsement function
			String createFunction_isValidEndorsement = 
					"create function isValidEndorsement("
					+ "CustomerID varchar(36),"
					+ "ReviewID varchar(36)"
					+ ") returns boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'StoredFunctions.isValidEndorsement'";
			stmt.executeUpdate(createFunction_isValidEndorsement);
			System.out.println("Created function isValidEndorsement");
			
			// create check verifyEndorsement function
			String createFunction_verifyEndorsement = 
					"create function verifyEndorsement("
					+ " EndorserID varchar(36),"
					+ " ReviewID varchar(36),"
					+ " checkDate date"
					+ ") returns boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'StoredFunctions.verifyEndorsement'";
			stmt.executeUpdate(createFunction_verifyEndorsement);
			System.out.println("Created function verifyEndorsement");
			
			// create check verifyAttendance function
			String createFunction_verifyAttendance = 
					"create function verifyAttendance("
					+ " CustomerID varchar(36),"
					+ " MovieID varchar(36),"
					+ " checkDate date"
					+ ") returns boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'StoredFunctions.verifyAttendance'";
			stmt.executeUpdate(createFunction_verifyAttendance);
			System.out.println("Created function verifyAttendance");
			
			// create check isOnlyReview function
			String createFunction_isOnlyReview = 
					"create function isOnlyReview("
					+ " CustomerID varchar(36),"
					+ " MovieID varchar(36)"
					+ ") returns boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'StoredFunctions.isOnlyReview'";
			stmt.executeUpdate(createFunction_isOnlyReview);
			System.out.println("Created function isOnlyReview");
			
			// create the isUuid function
			String createFunction_isUuid =
					" create function isUuid("
					+ " uuid varchar(36)"
					+ ") RETURNS boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'StoredFunctions.isUuid'";
			stmt.executeUpdate(createFunction_isUuid);
			System.out.println("Created function isUuid");
			
			// STORED FUNCTIONS FROM QUERIES.JAVA
			
			// create the checkLastEndorsementDate function
			String createFunction_checkLastEndorsementDate =
					" create function checkLastEndorsementDate("
					+ " ReviewID varchar(36),"
					+ " EndorserID varchar(36),"
					+ " EndorsementDate Date"
					+ ") RETURNS boolean"
					+ " language java"
					+ " parameter style java"
					+ " external name"
					+ " 'Queries.checkLastEndorsementDate'";
			stmt.executeUpdate(createFunction_checkLastEndorsementDate);
			System.out.println("Created function checkLastEndorsementDate");
			
			// create the Customer table
			String createTable_Customer =
					  "create table Customer ("
					+ "  Name varchar(36) not null,"
					+ "  Email varchar(36) not null,"
					+ "  Date date not null,"
					+ "  CustomerID varchar(36),"
					+ " primary key (CustomerID)"
					+ " )";
			stmt.executeUpdate(createTable_Customer);
			System.out.println("Created entity table Customer");
			
			// create the Movie table
			String createTable_Movie =
					  "create table Movie ("
					+ "  Title varchar(36) not null,"
					+ "  MovieID varchar(36) not null,"
					+ "  primary key (MovieID)"
					+ " )";
			stmt.executeUpdate(createTable_Movie);
			System.out.println("Created entity table Movie");
			
			// create the Attendance table
			String createTable_Attendance =
					  "create table Attendance ("
					+ " MovieID varchar(36) not null,"
					+ " CustomerID varchar(36) not null,"
					+ " Date date not null,"
					+ " foreign key (MovieID) references Movie (MovieID) on delete cascade"
					+ " )";
			stmt.executeUpdate(createTable_Attendance);
			System.out.println("Created entity table Attendance");
			
			// create the Review table
			String createTable_Review =
					  "create table Review ("
					+ " ReviewID varchar(36) not null unique,"
					+ " CustomerID varchar(36) not null,"
					+ " MovieID varchar(36) not null,"
					+ " ReviewDate date not null,"
					+ " Rating int not null,"
					+ " Review varchar(1000) not null,"
					+ " primary key (CustomerID, MovieID, ReviewDate),"
					+ " foreign key (MovieID) references Movie (MovieID) on delete cascade,"
					+ " foreign key (CustomerID) references Customer (CustomerID) on delete cascade,"
					+ " check(verifyAttendance(CustomerID, MovieID, ReviewDate)),"
					+ " check(isOnlyReview(CustomerID, MovieID))"
					+ " )";
			stmt.executeUpdate(createTable_Review);
			System.out.println("Created entity table Review");
			
			// create the Endorsement table
			String createTable_Endorsement =
					  "create table Endorsement ("
					+ " ReviewID varchar(36) not null,"
					+ " EndorserID varchar(36) not null,"
					+ " EndorsementDate date not null,"
					+ " primary key (ReviewID, EndorserID, EndorsementDate),"
					+ " foreign key (EndorserID) references Customer (CustomerID) on delete cascade,"
					+ " foreign key (ReviewID) references Review (ReviewID) on delete cascade,"
					+ " check(isValidEndorsement(EndorserID, ReviewID)),"
//					+ " check(verifyEndorsement(EndorserID, ReviewID, EndorsementDate))"
					+ " check(checkLastEndorsementDate(ReviewID, EndorserID, EndorsementDate)),"
					+ " )";
			stmt.executeUpdate(createTable_Endorsement);
			System.out.println("Created entity table Endorsement");
		
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	
}