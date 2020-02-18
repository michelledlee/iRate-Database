import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

//import org.apache.derby.szhared.common.reference.SQLState;


/**
 * The first part of this file tests the insertion of information into the database based on a separate data file.
 * 
 * The sample data is stored in a tab-separated data file The columns of the data file are:
 * Customer Name, Customer Email, CustomerID, Movie Title, MovieID, ReviewID, Rating, Review
 * 
 * The next part of the file tests the business intelligence queries that would be used by the theater to determine
 * prizes for the following:
 * 	1) Review with the most endorsements
 * 	2) Endorser with one or more endorsements on the same day
 * 
 * Further, there are utility queries that would assist the business in determine statistics and trends that they
 * could use to alter marketing strategies and make business decisions.
 * 
 * @author m lee
 */
public class Modelling {

	public static void main(String[] args) {
	    // the default framework is embedded
	    String protocol = "jdbc:derby:";
	    String dbName = "publication";
		String connStr = protocol + dbName+ ";create=true";

	    // tables tested by this program
		String dbTables[] = {
			"Customer", "Movie", "Attendance", "Review", "Endorsement"		// entities
	    };

		// name of data file
		String fileName = "theaterdata.txt";

		Properties props = new Properties(); // connection properties
        // providing a user name and password is optional in the embedded and derbyclient frameworks
        props.put("user", "user1");
        props.put("password", "user1");

        // result set for queries
        ResultSet rs = null;
        
        // TEST INSERTIONS INTO DATABASE
        
		try (
			// open data file
			BufferedReader br = new BufferedReader(new FileReader(new File(fileName)));
			
			// connect to database
			Connection  conn = DriverManager.getConnection(connStr, props);
			Statement stmt = conn.createStatement();
			
			// insert prepared statements
			PreparedStatement insertRow_Customer = conn.prepareStatement(
					"insert into Customer values(?, ?, ?, ?)");
			PreparedStatement insertRow_Movie = conn.prepareStatement(
					"insert into Movie values(?, ?)");
			PreparedStatement insertRow_Attendance = conn.prepareStatement(
					"insert into Attendance values(?, ?, ?)");
			PreparedStatement insertRow_Review = conn.prepareStatement(
					"insert into Review values(?, ?, ?, ?, ?)");
			PreparedStatement insertRow_Endorsement = conn.prepareStatement(
					"insert into Endorsement values(?, ?, ?)");
		) {
			// connect to the database using URL
            System.out.println("Connected to database " + dbName);
            
            // clear data from tables
            for (String tbl : dbTables) {
	            try {
	            	stmt.executeUpdate("delete from " + tbl);
	            	System.out.println("Truncated table " + tbl);
	            } catch (SQLException ex) {
	            	System.out.println("Did not truncate table " + tbl);
	            }
            }
            
			String line;
			while ((line = br.readLine()) != null) {
				// split input line into fields at tab delimiter
				String[] data = line.split("\t");
				if (data.length != 9) continue;
			
				// get fields from input data
				String customerName = data[0];
				String customerEmail = data[1];
				String customerID = data[2];

				// add Customer if does not exist
				try {
					insertRow_Customer.setString(1, customerName);
					insertRow_Customer.setString(2, customerEmail);
					insertRow_Customer.setString(3, customerID);
					insertRow_Customer.execute();
				} catch (SQLException ex) {
					// already exists
					 System.err.printf("Unable to insert Customer %s with email %s, and ID \n", customerName, customerEmail, customerID);
				}
				
				// get fields from input data
				String movieTitle = data[3];
				String movieID = data[4];	
				
				// add Movie if does not exist
				try {
					insertRow_Movie.setString(1, movieTitle);
					insertRow_Movie.setString(2, movieID);
					insertRow_Movie.execute();
				} catch (SQLException ex) {
					// already exists
					 System.err.printf("Unable to insert Movie %s \n", movieTitle);
				}

				// add Attendance if does not exist
				String attendanceMovieID = data[4];
				String attendanceCustomerID = data[2];
				try {
					insertRow_Attendance.setString(1, attendanceMovieID);
					insertRow_Attendance.setString(2, attendanceCustomerID);
					insertRow_Attendance.execute();
				} catch (SQLException ex) {
					// already exists
					System.err.printf("Unable to insert Attendance \"%s\" DOI %s\n", attendanceMovieID, attendanceCustomerID);
				}

				// add Review if does not exist
				String reviewID = data[5];
				String reviewcustomerID = data[2];
				String reviewMovieID = data[4];
				String reviewRating = data[6];
				String reviewReview = data[7];
				try {
					insertRow_Review.setString(1, reviewID);
					insertRow_Review.setString(2, reviewcustomerID);
					insertRow_Review.setString(3, reviewMovieID);
					insertRow_Review.setString(4, reviewRating);
					insertRow_Review.setString(5, reviewReview);
					insertRow_Review.execute();
				} catch (SQLException ex) {
					// already exists
					 System.err.printf("Unable to insert Review %s, %s ORCID %s\n", reviewID, reviewMovieID);
				}

			}
			
			// add Endorsements
			try {
				insertRow_Endorsement.setString(1, "00112233-4455-6677-8800-aabbccddeeff");
				insertRow_Endorsement.setString(2, "00112233-4455-6677-8899-aabbccddeeff");
				insertRow_Endorsement.setString(3, "CURRENT_DATE");
				insertRow_Endorsement.execute();
			} catch (SQLException ex) {
				// already exists
				 System.err.printf("Unable to insert Endorsement 1");
			}
			try {
				insertRow_Endorsement.setString(1, "00112233-4455-6677-8800-aabbccddeegg");
				insertRow_Endorsement.setString(2, "00112233-4455-6677-8899-aabbccddeeff");
				insertRow_Endorsement.setString(3, "CURRENT_DATE");
				insertRow_Endorsement.execute();
			} catch (SQLException ex) {
				// already exists
				 System.err.printf("Unable to insert Endorsement 2");
			}
			try {
				insertRow_Endorsement.setString(1, "00112233-4455-6677-8811-aabbccddeefg");
				insertRow_Endorsement.setString(2, "01112233-4455-6677-8899-aabbccddeeff");
				insertRow_Endorsement.setString(3, "CURRENT_DATE");
				insertRow_Endorsement.execute();
			} catch (SQLException ex) {
				// already exists
				 System.err.printf("Unable to insert Endorsement 3");
			}
			try {
				insertRow_Endorsement.setString(1, "00112233-4455-6677-8822-aabbccddeefg");
				insertRow_Endorsement.setString(2, "02112233-4455-6677-8899-aabbccddeeff");
				insertRow_Endorsement.setString(3, "CURRENT_DATE");
				insertRow_Endorsement.execute();
			} catch (SQLException ex) {
				// already exists
				 System.err.printf("Unable to insert Endorsement 4");
			}
			
			// print number of rows in tables
			for (String tbl : dbTables) {
				rs = stmt.executeQuery("select count(*) from " + tbl);
				if (rs.next()) {
					int count = rs.getInt(1);
					System.out.printf("Table %s : count: %d\n", tbl, count);
				}
			}
			rs.close();
			
			// TEST PRIZING QUERIES
			System.out.println("Testing prizing queries");

			Queries.mostEndorsements();	// Get most endorsed review for the day
			
			Queries.endorsementPrize();	// Get the most helpful votes for the day
			
			// TEST GENERAL BUSINESS INTELLIGENCE QUERIES
			System.out.println("Testing general business intelligence queries");

			Queries.totalReviews(); // Get total reviews in the database
			
			Queries.highestRated();	// Get the highest rated movies in the database
			
			Queries.mostReviews(); // Get the movie with the most reviews

			// TEST DELETIONS
			System.out.println("Testing deletions");
			// delete article
						System.out.println("\nDeleting customer 00112233-4455-6677-8899-aabbccddeeff (Rook Garbo)");
						stmt.execute("delete from Customer where CustomerID = '00112233-4455-6677-8899-aabbccddeeff'");
						Util.printCustomers(conn);
						Util.printCustomers(conn);

						// delete publisher ACM
						System.out.println("\nDeleting publisher ACM");
						stmt.executeUpdate("delete from Publisher where name = 'ACM'");
						PubUtil.printPublishers(conn);
						PubUtil.printJournals(conn);
						PubUtil.printArticles(conn);
						PubUtil.printAuthors(conn);
						
						// delete journal Spectrum (0018-9235)
						System.out.println("\nDeleting journal Spectrum from IEEE");
						stmt.executeUpdate("delete from Journal where issn = " + Biblio.parseIssn("0018-9235"));
						PubUtil.printJournals(conn);
						PubUtil.printArticles(conn);
						PubUtil.printAuthors(conn);
						
						
						// delete journal Computer
						System.out.println("\nDeleting journal Computer from IEEE");
						stmt.executeUpdate("delete from Journal where title = 'Computer'");
						PubUtil.printPublishers(conn);
						PubUtil.printJournals(conn);
						PubUtil.printArticles(conn);
						PubUtil.printAuthors(conn);
						
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
}