package com.test.bhaveshshah;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {
	private String jdbcURL = "jdbc:hsqldb:hsql://localhost/testdb";
	private String jdbcUsername = "SA";
	private String jdbcPassword = "";
	
	public Database() {
		try {
			Class.forName("org.hsqldb.jdbc.JDBCDriver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}		
	}

	public Connection getConnection() {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}	
	
	public void initialize() throws SQLException {
		this.createTable("create table IF NOT EXISTS eventlog (id varchar(100), duration integer, alert varchar(5));");
	}
	
	public void createTable(String createTableSQL) throws SQLException {
        try (Connection connection = this.getConnection();
            Statement statement = connection.createStatement();
        ) {
            statement.execute(createTableSQL);
        } catch (SQLException e) {
        	throw e;
        }
    }
	
	public void insertEventLog(String id, Long duration, String alert) throws SQLException {
        try (Connection connection = this.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("insert into eventLog values (?,?,?);");
        ) {
            preparedStatement.setString(1, id);
            preparedStatement.setLong(2, duration);
            preparedStatement.setString(3, alert);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
        	throw e;
        }		
	}	
}
