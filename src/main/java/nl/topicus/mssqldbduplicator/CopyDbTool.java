package nl.topicus.mssqldbduplicator;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class CopyDbTool {
	static Logger log = Logger.getLogger(CopyDbTool.class);
	
	protected Options options;
	
	protected CommandLine cmd;
	
	protected File credFile;
	
	protected String user;
	
	protected String password;
	
	protected String database;
	
	protected String name;
	
	protected String server;
	
	protected String instance;
	
	protected boolean overwrite;
	
	protected Connection conn;
	
	protected String backupDirectory;
	
	protected String dataDirectory;
	
	protected String backupFileName;
	
	protected String backupFilePath;
	
	protected String dataName;
	
	protected String logName;
	
	private String port;
	
	private boolean backupOnly;
	
	public CopyDbTool () {
		options = new Options();
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the credentials properties file");
		OptionBuilder.withLongOpt("credentials");
		options.addOption(OptionBuilder.create("c"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the username of the database user");
		OptionBuilder.withLongOpt("user");
		options.addOption(OptionBuilder.create("u"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the password of the database user");
		OptionBuilder.withLongOpt("password");
		options.addOption(OptionBuilder.create("p"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withLongOpt("database");
		OptionBuilder.withDescription("Specify the name of the database to copy");
		options.addOption(OptionBuilder.create("d"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("instance");
		OptionBuilder.withDescription("Specify the name of the instance to use on the SQL server");
		options.addOption(OptionBuilder.create("i"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("name");
		OptionBuilder.withDescription("Specify the name of the new copy");
		options.addOption(OptionBuilder.create("n"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withLongOpt("server");
		OptionBuilder.withDescription("Specify the hostname or IP address of the SQL server");
		options.addOption(OptionBuilder.create("s"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("port");
		OptionBuilder.withDescription("Specify the port on which sqlserver is listening (default: 1433)");
		options.addOption(OptionBuilder.create());
		
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("overwrite");
		OptionBuilder.withDescription("Flag to indicate whether or not to overwrite an existing database");
		options.addOption(OptionBuilder.create());
		
		OptionBuilder.hasArg(false);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("backuponly");
		OptionBuilder.withDescription("Flag to indicate if a database should only be backed up");
		options.addOption(OptionBuilder.create());

	}
	
	public String quoteIdentifier (String name) {
		name = name.replaceAll("\"",  "\\\\\"");
		name = "\"" + name + "\"";
		return name;
	}
	
	public String quoteValue (String value) {
		value = value.replaceAll("'", "\\\\'");
		value = "'" + value + "'";
		return value;
	}
	
	public String sanitizeFilename(String name) {
	    return name.replaceAll("[:\\\\/*?|<>]", "_");
	  }
	
	public Options getOptions () {
		return this.options;
	}
	
	public void run (String[] args) throws Exception {
		parseArgs(args);		
		
		openConnection();
		
		findSqlDirectories();
		
		// check if original database exists
		if (databaseExists(database) == false) {
			throw new Exception("Database " + quoteIdentifier(database) + " does not exist");
		}
		
		if (databaseExists(name)) {
			log.info("Another database already exists with name of copy");
			
			if (overwrite == false) {
				throw new Exception("Another database already exists with name of copy and overwrite is not enabled!");
			}
			
			// drop existing copy
			log.info("Dropping existing database with name of copy...");
			conn.createStatement().execute("DROP DATABASE " + quoteIdentifier(name));
			log.info("Database dropped");
		}
		
		// create backup
		createBackup();
		
		if (!backupOnly) {
			// get file info from backup
			findFileInfo();		
			
			// create copy database
			createCopy();
			
			// delete backup
			deleteBackup();
		}
		
		closeConnection();
		
		log.info("Finished!");
	}
	
	protected void deleteBackup () throws SQLException {
		log.info("Deleting backup...");
		
		PreparedStatement q = conn.prepareStatement("EXECUTE xp_delete_file 0, " + quoteValue(backupDirectory) + ", " + quoteValue(backupFileName));
		q.execute();
		
		q.close();
		
		log.info("Backup deleted");
	}
	
	protected void createCopy () throws SQLException {
		log.info("Creating copy database " + quoteIdentifier(name) + "...");
		
		String dataFilePath = dataDirectory + sanitizeFilename(name + ".mdf");
		String logFilePath = dataDirectory + sanitizeFilename(name + ".ldf");
		
		String sql = "RESTORE DATABASE " + quoteIdentifier(name) + " FROM DISK = N" + quoteValue(backupFilePath) + " " +
		"WITH MOVE " + quoteValue(dataName) + " TO " + quoteValue(dataFilePath) + ", MOVE " + quoteValue(logName) + " TO "
		+ quoteValue(logFilePath);
		
		PreparedStatement q = conn.prepareStatement(sql);
		q.execute();
		
		q.close();
		log.info("Copy created");
	}
	
	protected void findFileInfo () throws Exception {
		log.info("Finding file info of backup...");
		
		PreparedStatement q = conn.prepareStatement("RESTORE FILELISTONLY FROM DISK = N" + quoteValue(backupFilePath));
		ResultSet res = q.executeQuery();

		while(res.next()) {
			String type = res.getString("Type").toUpperCase();
			String logicalName = res.getString("LogicalName");
			
			if (type.equals("L")) {
				logName = logicalName;
			} else if (type.equals("D")) {
				dataName = logicalName;
			}
		}
		
		res.close();
		q.close();
		
		if (dataName == null || logName == null) {
			throw new Exception("Unable to find name of data file or log file in backup");
		}
		
		log.info("File info found");
	}
	
	protected void createBackup () throws SQLException {
		log.info("Creating backup of database " + quoteIdentifier(database) + "...");
		
		if (!backupOnly) {
			backupFileName = sanitizeFilename("backup-" + database + "-" + name + ".bak");
		} else {
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			Calendar cal = Calendar.getInstance();
			backupFileName = sanitizeFilename(dateFormat.format(cal.getTime()) + database + ".bak");
		}
		
		backupFilePath = backupDirectory + backupFileName;
		
		PreparedStatement q = conn.prepareStatement("BACKUP DATABASE " + quoteIdentifier(database) + " TO DISK = N" + quoteValue(backupFilePath));
		q.execute();
		
		q.close();
		log.info("Backup created");
	}
		
	protected boolean databaseExists (String name) throws SQLException {
		PreparedStatement q = conn.prepareStatement("SELECT * FROM sysdatabases WHERE name = ?");
		q.setString(1,  name);
		
		ResultSet res = q.executeQuery();
		
		boolean ret = res.next();
		
		res.close();
		q.close();
		
		return ret;
	}
	
	protected void findSqlDirectories () throws Exception {
		Statement q = conn.createStatement();
				
		ResultSet res = q.executeQuery("EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',N'BackupDirectory'");
		
		if (res.next()) {
			String dir = res.getString("Data");
			backupDirectory =  FilenameUtils.getFullPath(dir);
		}
		
		res.close();
		
		res = q.executeQuery("SELECT physical_name FROM sys.database_files");
		
		if (res.next()) {
			String dataFilePath = res.getString("physical_name");
			dataDirectory = FilenameUtils.getFullPath(dataFilePath);
		}		
		
		res.close();
		q.close();
		
		if (StringUtils.isEmpty(backupDirectory)) {
			throw new Exception("Unable to find SQL Server backup directory");
		}
		
		if (StringUtils.isEmpty(dataDirectory)) {
			throw new Exception("Unable to find SQL Server data directory");
		}
		
		log.debug("Found SQL server backup directory: " + backupDirectory);
		log.debug("Found SQL server data directory: " + dataDirectory);
	}
	
	protected void closeConnection () throws SQLException {
		if (conn != null && conn.isClosed() == false) {
			conn.close();
			log.info("Closed connection to SQL server");
		}		
	}
	
	protected void openConnection () throws SQLException, ClassNotFoundException {
		Class.forName("net.sourceforge.jtds.jdbc.Driver");
		
		Properties connProps = new Properties();
		
		if (user != null && password != null) {
			connProps.setProperty("user",  user);
			connProps.setProperty("password", password);
		}
		
		if (instance != null){
			connProps.setProperty("instance", instance);
		}
		
		String url = "jdbc:jtds:sqlserver://" + server + ":" + port +"/master";
		log.debug("Using connection URL for MS SQL Server: " + url);
		

		conn = DriverManager.getConnection(url, connProps);
		log.info("Opened connection to MS SQL Server");	
	}
	
	protected void parseArgs (String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		cmd = parser.parse( options, args);
		
		// check for credentials
		if (StringUtils.isEmpty(cmd.getOptionValue("credentials")) == false) {
			File credFile = new File(cmd.getOptionValue("credentials"));
			
			if (credFile.exists() == false || credFile.canRead() == false) {
				throw new Exception("Unable to read credentials file '" + credFile.getPath() + "'");
			}
			
			Properties config = new Properties();
			config.load(new FileInputStream(credFile));
			
			if (config.containsKey("user")) {
				user = config.getProperty("user");
			}
			
			if (config.containsKey("password")) {
				password = config.getProperty("password");
			}
		}
		
		if (StringUtils.isEmpty(cmd.getOptionValue("user")) == false) {
			user = cmd.getOptionValue("user");
		}
		
		if (StringUtils.isEmpty(cmd.getOptionValue("password")) == false) {
			password = cmd.getOptionValue("password");
		}
		
		if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
			throw new MissingCredentialsException("Missing credentials (user/password)");
		}
		
		server = cmd.getOptionValue("server");
		log.info("SQL server: " + server);
		
		port = cmd.getOptionValue("port");
		if (StringUtils.isEmpty(port)) {
			port = "1433";
		}
		log.info("Port: " + port);
		
		log.info("User: " + user);
		
		instance = cmd.getOptionValue("instance");
		if (StringUtils.isEmpty(instance) == false) {
			log.info("Instance: " + instance);
		}
		
		database = cmd.getOptionValue("database");
		log.info("Database to copy: " + database);
		
		name = cmd.getOptionValue("name");
		log.info("Name of database copy: " + name);	
		
		overwrite = (cmd.hasOption("overwrite"));
		if (overwrite) {
			log.info("Overwrite enabled");
		}
		
		backupOnly = (cmd.hasOption("backuponly"));
		if (backupOnly) {
			log.info("backup only");
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		BasicConfigurator.configure();
		log.info("Started MSSQL-DB-Duplicator copy tool");
		
		CopyDbTool tool = new CopyDbTool();
		
		try {
			tool.run(args);
		} catch (ParseException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.out.println("");
			
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(" ", tool.getOptions());
			System.exit(1);
		} catch (Exception e) {
			log.fatal(e.getMessage(), e);
			System.exit(1);
		}		
		
		// clean exit
		System.exit(0);		
	}
	
	
	public class MissingCredentialsException extends Exception {
		private static final long serialVersionUID = 1L;

		public MissingCredentialsException(String string) {
			super(string);
		}
	}

}
