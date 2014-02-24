package com.datastax.lwt;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class LwtExample {

	private static Logger logger = LoggerFactory.getLogger(LwtExample.class);

	private static final int TOTAL_RECORDS = 1000;

	private final Session session;

	private PreparedStatement updateLwtStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement findStmt;

	private boolean debug = false;

	private boolean LWT;

	private static final String testTableName = "test_table";
	private static final String keyspaceName = "test_keyspace_lwt";
	private static final String tableName = keyspaceName + "." + testTableName;
	private static final String CREATE_KEYSPACE = "CREATE KEYSPACE " + keyspaceName
			+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }";
	private static final String DROP_KEYSPACE = "DROP KEYSPACE " + keyspaceName;

	private static final String CREATE_TABLE = "CREATE TABLE " + tableName
			+ "(user_id text PRIMARY KEY, first text, last text, city text, email text)";
	private static final String INSERT_CQL = "INSERT INTO " + tableName
			+ "(user_id, first, last, city, email) VALUES (?, ?, ?, ?, ?)";
	private static final String UPDATE_LWT_CQL = "UPDATE " + tableName + " set email=? where user_id = ? IF email = ?";
	private static final String UPDATE_CQL = "UPDATE " + tableName + " set email=? where user_id = ?";

	public LwtExample() {

		Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
		this.session = cluster.connect();

		debug = Boolean.parseBoolean(PropertyHelper.getProperty("debug", "false"));
		LWT = Boolean.parseBoolean(PropertyHelper.getProperty("LWT", "true"));

		logger.info("Cluster and Session created.");

		this.setUp();
		this.createTable();

		updateLwtStmt = session.prepare(UPDATE_LWT_CQL);
		updateStmt = session.prepare(UPDATE_CQL);
		findStmt = session.prepare("SELECT email from " + this.tableName + " where user_id = ?");

		Timer timer = new Timer();
		timer.start();

		for (int i = 0; i < TOTAL_RECORDS; i++) {
			String userId = "U" + i;
			this.updateEmailFailed(userId);
		}
		timer.end();
		this.tearDown();

		logger.info("Lightweight Transaction test finished. 1000 records finished in " + timer.getTimeTakenMillis()
				+ "ms");

		cluster.shutdown();
	}

	private void createTable() {
		// Set up ColumnFamily

		this.session.execute(CREATE_TABLE);
		logger.info("Table " + tableName + " created");

		PreparedStatement stmt = session.prepare(INSERT_CQL);
		BoundStatement boundStmt = new BoundStatement(stmt);

		// populate
		for (int i = 0; i < TOTAL_RECORDS; i++) {
			session.execute(boundStmt.bind("U" + i, "first" + i, "last" + i, "city" + i, "email@gmail.com" + i));
		}

		logger.info("Table Populated with " + TOTAL_RECORDS + " records");
	}

	private void updateEmail(String userId) {

		updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateBoundStmt = new BoundStatement(updateLwtStmt);
		this.session.execute(updateBoundStmt.bind("updatedtosomethingelse@gmail.com", userId));
	}

	private void updateEmailWithLWTWithoutFind(String userId) {

		updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateLwtBoundStmt = new BoundStatement(updateLwtStmt);

		BoundStatement findBoundStmt = new BoundStatement(findStmt);

		ResultSet resultSet = this.session.execute(findBoundStmt.bind(userId));
		this.session.execute(updateLwtBoundStmt.bind("newemail@gmail.com", userId, "email@gmail.com"));

	}

	private void updateEmailWithLWT(String userId) {

		updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateLwtBoundStmt = new BoundStatement(updateLwtStmt);

		BoundStatement findBoundStmt = new BoundStatement(findStmt);

		ResultSet resultSet = this.session.execute(findBoundStmt.bind(userId));

		if (resultSet != null) {
			Row row = resultSet.one();
			String currentEmail = row.getString("email");

			logger.info("Current Email was " + currentEmail);
			this.session.execute(updateLwtBoundStmt.bind("newemail@gmail.com", userId, currentEmail));
		}

		ResultSet newResultSet = this.session.execute(findBoundStmt);
		if (resultSet != null) {
			Row row = newResultSet.one();
			String currentEmail = row.getString("email");
			logger.info("Current Email is now " + currentEmail);
		}
	}

	private void updateEmailFailed(String userId) {
		this.updateLwtStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateLwtBoundStmt = new BoundStatement(updateLwtStmt);

		// Update without LWT
		this.updateStmt = session.prepare(UPDATE_CQL);
		updateStmt.setConsistencyLevel(ConsistencyLevel.QUORUM);
		BoundStatement updateBoundStmt = new BoundStatement(updateStmt);

		BoundStatement findBoundStmt = new BoundStatement(findStmt);

		ResultSet resultSet = this.session.execute(findBoundStmt.bind(userId));

		if (resultSet != null) {
			Row row = resultSet.one();
			String currentEmail = row.getString("email");
			logger.info("Current Email was " + currentEmail);

			// Intercept and udpate the email before trying to do update with
			// lwt.
			this.session.execute(updateBoundStmt.bind("updatedtosomethingelse@gmail.com", userId));

			// This execute should now fail with row of different values
			ResultSet shouldFailResultSet = this.session.execute(updateLwtBoundStmt.bind("newemail@gmail.com", userId,
					currentEmail));

			if (shouldFailResultSet != null) {
				Row failedRow = shouldFailResultSet.one();				
				
				if (failedRow.getBool(0) == false) {
					String foundEmail = failedRow.getString("email");
					logger.info("Update failed as current email was " + foundEmail + " not " + currentEmail);
					Assert.assertFalse(failedRow.getBool(0));
				}else{
					logger.info("Update Succeeded");
					Assert.fail("Email update should fail");
				}
			}
		}
	}

	public void setUp() {
		// Set up Keyspace
		this.session.execute("DROP KEYSPACE IF EXISTS " + keyspaceName);
		this.session.execute(CREATE_KEYSPACE);
		logger.info("Keyspace " + keyspaceName + " created");
	}

	public void tearDown() {

		this.session.execute(DROP_KEYSPACE);
		logger.info("Keyspace DROPPED");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new LwtExample();
	}

}
