import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.PreparedStatement;
import java.sql.DriverManager;


/**
 * This class contains functions for executing SQL queries on the database. The functions include:
 * 	1) Printing the customerID that had the review with the most endorsements
 * 	2) Printing the customerID that had endorsed one or more reviews on a given day
 * 	3) Query number of reviews
 * 	4) Query the highest rated movies
 *  5) Query movie with the most reviews
 *  6) Given a ReviewID, query for the CustomerID of the person who wrote the review
 *  
 *  These queries provide a quick way for the theater to pull statistics regarding the engagement
 *  levels of users to determine whether the rating system is working to encourage more viewership.
 *
 *	@author m lee
 */
public class Queries {

	/**
	 * Prints the CustomerID that had the review with the most endorsements.
	 * @return the CustomerID of the person with the most endorsements
	 * @throws SQLException if a database operation fails
	 */
	public static String mostEndorsements() throws SQLException {
		try (
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
				
			// create statement using connection 
			Statement stmt = conn.createStatement();
			
			// gets review with the most endorsements within 3 days
			ResultSet rs = stmt.executeQuery(
					"select CustomerID "										// selects the CustomerID which will be the winner
					+ "from Review "											// from the Review table
					+ "left join Endorsement on Review = Endorsement.ReviewID "	// combines matching rows from Endorsement based on the shared ReviewID field
					+ "group by ReviewID "										// aggregating results based on ReviewID because the most endorsed review should show up the most in the Endorsement table
					+ "where ReviewDate >= cast(getdate() as date)"	// only considering reviews from today's date; later functionality would include the ability to specify a date
					+ "order by count(ReviewID) desc "				// order by the count of the ReviewID since the most endorsed review should be there the most and sort desc to get the highest total
					+ "limit 1");									// only getting the top result back since there can be only one winner; later functionality would randomize if there were more than one reviews with the highest total of endorsements
		) {
			String customerID = rs.getString(1);
			System.out.println("Selected winner of a free movie ticket is CustomerID: " + customerID);
			return customerID;
		}
		
	}
	
	/**
	 * Prints the CustomerID of the person who will receive a free concession item from voting on one or more reviews as "helpful".
	 * @return the CustomerID of the person selected for the concession prize 
	 * @throws SQLException if a database operation fails
	 */
	public static String endorsementPrize() throws SQLException {
		try (
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
				
			// create statement using connection 
			Statement stmt = conn.createStatement();
			
			// gets EndorsementID of the person who won the concession prize
			ResultSet rs = stmt.executeQuery(
				"select EndorserID"				// get the EndorserID which will be the winner
				+ "from Endorsement "			// from the Endorsement table
				+ "group by EndorserID desc "	// aggregating results based on EndorserID since we want to know how many a person did in one day
				+ "where (count(EndorserID) > 1) AND (EndorsementDate = cast(getdate() as date)"	// filtering results so that the person did at least 1 or more reviews and for today's date
				+ "limit 1");	// limits the result to one since we can only have one winner; later functionality would randomize the selection better as right now it is biased towards higher EndorserIDs
		) {
			String endorsementID = rs.getString(1);
			System.out.printf("Selected winner of a free concessions is EndorsementID %s\n", endorsementID);
			return endorsementID;
		}		
	}
	
	/**
	 * Queries the database for the total number of reviews. This is a business intelligence query about the social media database platform.
	 * For example, management could monitor the growth of the reviews over time to determine engagement levels.
	 * @return the # of reviews in the database
	 * @throws SQLException if a database operation fails
	 */
	public static int totalReviews() throws SQLException {
		try (
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
				
			// create statement using connection 
			Statement stmt = conn.createStatement();
				
			// gets count of reviews
			ResultSet rs = stmt.executeQuery(
				"select count(*) "	// returns the # of reviews by counting the rows
				+ "from Review");	// queries the Review table
		) {
			int numOfRows = rs.getInt(1);
			System.out.println("Total # of reviews: " + numOfRows);
			return numOfRows;
		}		
	}

	/**
	 * Queries the database for the highest rated movie(s). This is a business intelligence query. Management could use this information to 
	 * pull back on advertising for movies that are already doing well.
	 * @throws SQLException if a database operation fails
	 */
	public static void highestRated() throws SQLException {
		try (
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
				
			// create statement using connection 
			Statement stmt = conn.createStatement();
				
			// gets the highest rated movies from the database 
			ResultSet rs = stmt.executeQuery(
				"select MovieID, max(Rating) "	// get the MovieID with the highest rating
				+ "from Review "				// from the Review table
				+ "group by MovieID");			// aggregate by MovieID since this is the only thing we need
		) {
			System.out.println("Highest rated movies: ");
			// there may be several movies with the same rating, so print all
			while (rs.next()) {
				String movieName = rs.getString(1);
				String movieRating = rs.getString(2);
				System.out.printf("%s, %s\n", movieName, movieRating);
			}
		}		
	}
	
	/**
	 * Queries the database for the movie(s) with the most reviews.
	 * @throws SQLException if a database operation fails
	 */
	public static void mostReviews() throws SQLException {
		try (
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
				
			// create statement using connection 
			Statement stmt = conn.createStatement();
				
			// gets the movie with the most reviews
			ResultSet rs = stmt.executeQuery(
				"select MovieID, count(ReviewID) as reviews "	// return the MovieID and associated count of the reviews
				+ "from Review "								// from the Review table
				+ "group by MovieID "							// for the ReviewID counts, group based on MovieID
				+ "order by reviews desc");						// list from most reviews to the fewest
		) {
			// there may be several movies with the same # of reviews
			System.out.println("Movies with the most reviews: ");
			while (rs.next()) {
				String movieName = rs.getString(1);
				System.out.printf("%s\n", movieName);
			}
		}		
	}
	
		
	/**
	 * Checks that a customer can only endorse the same movie once per day.
	 * @param reviewID ReviewID from the endorsement being input
	 * @param endorserID EndorserID from the endorsement being input
	 * @param endorsementDate EndorsementDate from the endorsement being input
	 * @return returns true 
	 * @throws SQLException if a database operation fails
	 * @throws ParseException if a date cannot be parsed
	 */
	public static boolean checkLastEndorsementDate(String reviewID, String endorserID, Date endorsementDate) throws SQLException, ParseException {
		try {
			// get connection to the database
			Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
			
			// look up the MovieID
			PreparedStatement stmt1 = conn.prepareStatement(
					"select MovieID from Review where ReviewID = ?");
			stmt1.setString(1, reviewID);
			ResultSet rs1 = stmt1.executeQuery();
			String movieID = rs1.getString("MovieID");
	
			// look up the last date this endorser has endorsed the same movie
			PreparedStatement stmt2 = (PreparedStatement) conn.prepareStatement(
					"select coalesce(Endorsement.EndorsementDate, '1970-01-01') "	// gets the soonest date the endorser has endorsed the same movie
																					// or if none is found, returns a default value
					+ "from Endorsement "											// based on the Endorsement table
					+ "left join Review on Endorsement.ReviewID = Review.ReviewID "	// joins the Review and Endorsement tables based on the ReviewID so
																					// we can look up across EndorserID and MovieID
					+ "where Review.MovieID = ? and EndorserID = ? "	// only interested in the movie that the endorser is trying to endorse
																	// by the endorser in question
					+ "group by Endorsement.EndorserID "				// groups the results based on the endorser ID that we are interested in
					+ "order by Endorsement.EndorsementDate desc "	// orders by most recent date
					+ "limit 1");	// only interested in the most recent date, ignore other results
			stmt2.setString(1, movieID);
			stmt2.setString(1, endorserID);
	
			// get the date result that was returned
			Date defaultDate = (Date) new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
			ResultSet rs2 = stmt2.executeQuery();
			Date lastEndorsement = rs2.getDate(1);
			
			
			if (lastEndorsement.after(defaultDate)) {
				System.out.println("The last endorsement date was: " + lastEndorsement);
				return true;
			} 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	// LIST OF ALTERNATIVE QUERIES
	
	/**
	 * Alternate Option
	 * 
	 * Gets the date of last endorsement for someone endorsing a review. It can be called when a new endorsement is being
	 * created to retrieve the last attendance which can be used by the StoredFunctions.checkOneDay(Date date, Date oldDate) function. 
	 * @param MovieID this is the movie's MovieID 
	 * @return Date this the the last attendance value which can be used 
	 * @throws SQLException if a database operation fails
	 */
	public static Date getLastEndorsement(String movieID, String customerID) throws SQLException {
		// get connection to the database
		Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
		
		// prepared statement accepts the MovieID and CustomerID that would be extracted from the review and queries the database for the 
		// last attendance date based on these values
		PreparedStatement stmt = (PreparedStatement) conn.prepareStatement(
				"select Date from Attendance where where MovieID = ? AND CustomerID = ?");	
		stmt.setString(1, movieID);
		stmt.setString(2, customerID);
		
		// get the customerID and movieID to query attendance 
		ResultSet rs = stmt.executeQuery();
		Date lastAttendance = rs.getDate(1);
		System.out.println(lastAttendance);
		return lastAttendance;
		
	}
	
	/**
	 * Alternate Option - Can be used in conjunction with the StoredFunctions.checkSevenDays(Date oldDate, Date newDate) function 
	 * to verify whether a review has been written with 7 days of attendance.
	 * 
	 * Gets the date of last attendance for someone writing a review. 
	 * @param MovieID this is the movie's MovieID 
	 * @return Date this the the last attendance value which can be used 
	 * @throws SQLException if a database operation fails
	 */
	static Date getLastAttendance(String MovieID, String CustomerID) throws SQLException {
		// get connection to the database
		Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
		
		// prepared statement accepts the MovieID and CustomerID that would be extracted from the review and queries the database for the 
		// last attendance date based on these values
		PreparedStatement stmt = (PreparedStatement) conn.prepareStatement(
				"select Date from Attendance where where MovieID = ? AND CustomerID = ?");	
		stmt.setString(1, MovieID);
		stmt.setString(2,  CustomerID);
		
		// get the customerID and movieID to query attendance 
		ResultSet rs = stmt.executeQuery();
		Date lastAttendance = rs.getDate(1);
		System.out.println(lastAttendance);
		return lastAttendance;
	}
	
	/**
	 * Alternate Option - Can be used instead of Storedfunctions.verifyAttendance(String customerId, String movieId, Date date).
	 * 
	 * Checks that the date of a review is within 7 days of most recent attendance.
	 * @param conn the connection
	 * @return number of articles
	 * @throws SQLException if a database operation fails
	 */
	static boolean validReviewDate(String ReviewID) throws SQLException {	
		// get connection to the database
		Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
		
		// prepared statement returns the CustomerID and MovieID for a given ReviewID
		PreparedStatement stmt1 = (PreparedStatement) conn.prepareStatement(
				"select CustomerID, MovieID from Review where ReviewID = ?");	
		stmt1.setString(1, ReviewID);
		// get the customerID and movieID to query attendance 
		ResultSet rs1 = stmt1.executeQuery();
		String customerID = rs1.getString(1);
		String movieID = rs1.getString(2);
		System.out.println("CustomerID: " + customerID + "MovieID: " + movieID);
	
		// prepared statement returns the date that this customer in question attended which movie
		PreparedStatement stmt2 = (PreparedStatement) conn.prepareStatement(
				"select Date from Attendance where (CustomerID = ?) AND (MovieID = ?)");	
		stmt2.setString(1, customerID);
		// get the date of most recent attendance by that user
		ResultSet rs2 = stmt2.executeQuery();
		String attendanceDate = rs2.getString(1);
		System.out.println("Latest Attendance Date: " + attendanceDate);
		
		// compare the dates and return whether the the date is valid
		PreparedStatement stmt3 = (PreparedStatement) conn.prepareStatement(
				"select datediff(day, convert(date, getdate()), ?) as DateDiff");	
		stmt3.setString(1, attendanceDate);
		// get current date from database
		ResultSet rs3 = stmt3.executeQuery();
		int days = rs3.getInt(1);
		
		return days <= 7 ? true : false;
		
	}
	
	/**
	 * ALTERNATE OPTION - Can be used instead of StoredFunctions.isValidEndorsement(String customerID, String endorserID)
	 * 
	 * Checks that a customer has not endorsed their own review.
	 * @param endorserID the EndorserID of the person endorsing the review
	 * @throws SQLException if a database operation fails
	 */
	static boolean endorsedOwnReviewCheck(String endorserID, String reviewID) throws SQLException {
		// get connection to the database
		Connection conn = DriverManager.getConnection("jdbc:default:connection"); 
			
		// create statement using connection 
		PreparedStatement stmt1 = conn.prepareStatement(
				"select CustomerID "
				+ "from Review "
				+ "where ReviewID = ?");
		stmt1.setString(1, reviewID);
		ResultSet rs1 = stmt1.executeQuery();
		String customerID = rs1.getString("CustomerID");
		
		return customerID == endorserID ? false : true;
		
	}
	
}