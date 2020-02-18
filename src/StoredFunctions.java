import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Support methods to be used by stored functions for checking
 * validity of dates. 
 * 
 * @author b garbo
 *
 */
public class StoredFunctions {
	
	/** 
	 * Determines whether a string conforms to the pattern for a UUID.
	 * 
	 * @param uuid the UUID string
	 * @return true if the string is a valid UUID
	 */
	static public boolean isUuid(String uuid) {
		return uuid.matches("^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$");
	}
	
	/**
	 * Determine if an endorsement on a review is valid. A customer 
	 * cannot endorse their own review.
	 * 
	 * @param customerId the ID of the endorser
	 * @param reviewId the review to endorse
	 * @return true if the review was not written by the endorser 
	 */
	static public boolean isValidEndorsement(String customerId, String reviewId) {
		try {
			Connection conn = ProjectMain.getConnection();
			PreparedStatement stmt = conn.prepareStatement(
					"select CustomerID from Review where ReviewID = ?");
			
			stmt.setString(1, reviewId);
			ResultSet rs = stmt.executeQuery();
			
			String checkId = rs.getString("CustomerID");
			
			if (checkId != customerId) {
				return true;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 *  Verify that a customer has attended a movie they would like to
	 *  write a review for.
	 *  
	 * @param customerId the customer writing the review
	 * @param movieId the movie they are reviewing
	 * 
	 * @return true if the customer attended the movie
	 */
	static public boolean verifyAttendance(String customerId, String movieId, Date date) {
		try {
			Connection conn = ProjectMain.getConnection();
			PreparedStatement stmt = conn.prepareStatement(
					"select MovieID, Date from Attendance where CustomerID = ?");
			
			stmt.setString(1, customerId);
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()){
				String checkId = rs.getString("MovieID");
					if (checkId == movieId) {
						Date attendanceDate = rs.getDate("Date");
						if (checkSevenDays(date, attendanceDate)) {
							return true;
						}
					}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Check that there are no other reviews for this movie
	 * written by this customer. 
	 * 
	 * @param customerId the customer writing the review
	 * @param movieId the movie the customer is reviewing
	 * @return true if they have not previously submitted a review
	 */
	static public boolean isOnlyReview(String customerId, String movieId) {
		try {
			Connection conn = ProjectMain.getConnection();
			PreparedStatement stmt = conn.prepareStatement(
					"select CustomerId from Review where MovieID = ?");
			
			stmt.setString(1, movieId);
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()){
				String checkId = rs.getString("CustomerID");
					if (checkId == customerId) {
						return false;
					}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * Check if a given date is within seven days of an older date.
	 * 
	 * @param date the date to check
	 * @param oldDate the older date 
	 * @return true if the given date is within seven days of the older date
	 */
	static public boolean checkSevenDays(Date date, Date oldDate) {
			LocalDate checkDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			checkDate = checkDate.minusDays(7);
			LocalDate olderDate = oldDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			
			if (checkDate.compareTo(olderDate) >= 0) {
				return true;
			}
			else return false;
	}
	
	/**
	 * Check if a given date is within three days of an older date.
	 * 
	 * @param date the date to check
	 * @param oldDate the older date 
	 * @return true if the given date is within three days of the older date
	 */
	static public boolean checkThreeDays(Date date, Date oldDate) {
		LocalDate checkDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		checkDate = checkDate.minusDays(3);
		LocalDate olderDate = oldDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		
		if (checkDate.compareTo(olderDate) >= 0) {
			return true;
		}
		else return false;
	}
	
	
	/**
	 * Check if a given date is within one day of an older date.
	 * 
	 * @param date the date to check
	 * @param oldDate the older date 
	 * @return true if the given date is within one of the older date
	 */
	static public boolean checkOneDay(Date date, Date oldDate) {
		LocalDate checkDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		checkDate = checkDate.minusDays(1);
		LocalDate olderDate = oldDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		
		if (checkDate.compareTo(olderDate) >= 0) {
			return true;
		}
		else return false;
	}
	
	/**
	 * Alternate Option - Was a work in progress for corresponding Queries.checkLastEndorsementDate(String reviewID, 
	 * String endorserID, Date endorsementDate)
	 * 
	 * Determines if, for a given movie, a submitted endorsement for a review 
	 * by a customer is at least one day after a customer's endorsement of
	 * a review for the same movie.
	 * 
	 * @param customerId the customer endorsing a review
	 * @param reviewId the review they are endorsing
	 * @param date the date of the endorsement
	 * 
	 * @return true if it has been at least one day since this customer
	 * endorsed a review of the same movie
	 */
	static public boolean verifyEndorsement(String customerId, String reviewId, Date date) {
		try {
			Connection conn = ProjectMain.getConnection();
			PreparedStatement stmt = conn.prepareStatement(
					"select MovieID from Review where ReviewID = ?");
			
			stmt.setString(1, reviewId);
			ResultSet rs = stmt.executeQuery();
			
			// store the movie
			String checkMovie = rs.getString("MovieID");
			
//			stmt = conn.prepareStatement(
//					"--");
//			
//			stmt.setString(1, reviewId);
//			ResultSet rs = stmt.executeQuery();
//			
//			if (checkMovie != customerId) {
//				return true;
//			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
	
}