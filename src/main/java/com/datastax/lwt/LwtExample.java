package com.datastax.lwt;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class LwtExample {
	
	private static final int TOTAL_RECORDS = 10000;
	
	private final Session session;

	private static final String testTableName = "test_table";
	private static final String keyspaceName = "test_keyspace_lwt";
	private static final String tableName = keyspaceName + "." + testTableName;	
	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE " + keyspaceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }";
	private static final String DROP_KEYSPACE = "DROP KEYSPACE " + keyspaceName;
	
	private static final String CREATE_TABLE = "CREATE TABLE " + tableName + "(user_id text PRIMARY KEY, first text, last text, city text, email text)";
	private static final String INSERT_CQL = "INSERT INTO " + tableName + "(user_id, first, last, city, email) VALUES (?, ?, ?, ?, ?)";
	private static final String UPDATE_LWT_CQL = "UPDATE " + tableName + " set email=? where user_id = ? IF email = ?";
	private static final String UPDATE_CQL = "UPDATE " + tableName + " set email=? where user_id = ?";
	
	public LwtExample(){
			
		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();		
		this.session = cluster.connect();
		
		System.out.println("Cluster and Session created.");
		
		this.setUp();
		this.createTable();
		this.updateEmail();
		this.updateEmailFailed();
		this.tearDown();
		
		System.out.println("Lightweight Transaction test finished.");
		
		cluster.shutdown();
	}

	private void createTable() {
		//Set up ColumnFamily

		this.session.execute(CREATE_TABLE);
		System.out.println("Table " + tableName + " created");		

		PreparedStatement stmt = session.prepare(INSERT_CQL );
		BoundStatement boundStmt = new BoundStatement(stmt);
		
		//populate 
		for (int i = 0; i < TOTAL_RECORDS; i++){
			session.execute(boundStmt.bind("U" + i, "first" + i, "last" + i, "city" + i ,"email@gmail.com" + i));
		}
		
		System.out.println("Table Populated with " + TOTAL_RECORDS + " records");
	}

	private void updateEmail(){
		PreparedStatement updateLwtStmt = session.prepare(UPDATE_LWT_CQL);
		updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateLwtBoundStmt = new BoundStatement(updateLwtStmt);
		
		PreparedStatement findStmt = session.prepare("SELECT email from " + this.tableName + " where user_id = 'U1001'");
		BoundStatement findBoundStmt = new BoundStatement(findStmt);
				
		ResultSet resultSet = this.session.execute(findBoundStmt.bind());
		
		if (resultSet != null){
			Row row = resultSet.one();
			String currentEmail = row.getString("email");
			System.out.println ("Current Email was " + currentEmail);
			
			this.session.execute(updateLwtBoundStmt.bind("newemail@gmail.com", "U1001", currentEmail));
		}		
		
		ResultSet newResultSet = this.session.execute(findBoundStmt);
		if (resultSet != null){
			Row row = newResultSet.one();
			String currentEmail = row.getString("email");
			System.out.println ("Current Email is now " + currentEmail);
		}
	}
	
	private void updateEmailFailed(){
		PreparedStatement updateLwtStmt = session.prepare(UPDATE_LWT_CQL);
		updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateLwtBoundStmt = new BoundStatement(updateLwtStmt);

		//Update without LWT
		PreparedStatement updateStmt = session.prepare(UPDATE_CQL);
		updateStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateBoundStmt = new BoundStatement(updateStmt);
		
		PreparedStatement findStmt = session.prepare("SELECT email from " + this.tableName + " where user_id = 'U1001'");
		BoundStatement findBoundStmt = new BoundStatement(findStmt);
				
		ResultSet resultSet = this.session.execute(findBoundStmt.bind());
		
		if (resultSet != null){
			Row row = resultSet.one();
			String currentEmail = row.getString("email");
			System.out.println ("Current Email was " + currentEmail);
			
			//Intercept and udpate the email before trying to do update with lwt.
			this.session.execute(updateBoundStmt.bind("updatedtosomethingelse@gmail.com", "U1001"));

			//This execute should now fail with row of different values
			ResultSet failedResultSet = this.session.execute(updateLwtBoundStmt.bind("newemail@gmail.com", "U1001", currentEmail));
			
			if (failedResultSet != null){
				Row failedRow = failedResultSet.one();
				String foundEmail = failedRow.getString("email");
				
				System.out.println("Update failed as current email was " + foundEmail + " not " + currentEmail);
			}
		}				
	}
	
	public void setUp(){		
		//Set up Keyspace
		this.session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);		
		this.session.execute(CREATE_KEYSPACE );
		System.out.println("Keyspace " + keyspaceName + " created");		
	}
	
	
	public void tearDown(){
				
		this.session.execute(DROP_KEYSPACE);
		System.out.println("Keyspace DROPPED");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new LwtExample();
	}

}
