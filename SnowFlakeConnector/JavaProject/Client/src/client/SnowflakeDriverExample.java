package client;


import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.jws.Oneway;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

import javax.xml.ws.BindingType;
import javax.xml.ws.soap.SOAPBinding;

@WebService(name = "Snowflake", serviceName = "SnowflakeService", portName = "SnowflakePort")
@BindingType(SOAPBinding.SOAP12HTTP_BINDING)
public class SnowflakeDriverExample
{
    @WebMethod(exclude = true)
    public static void main(String[] args) throws Exception
  {
      SnowflakeDriverExample example=new SnowflakeDriverExample();
      example.insertRow("G OIC Val");
  }
   private static Connection getConnection()
          throws SQLException
  {
    try
    {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    }
    catch (ClassNotFoundException ex)
    {
     System.err.println("Driver not found");
    }
    // build connection properties
    Properties properties = new Properties();
    properties.put("user", "");     // replace "" with your username
    properties.put("password", ""); // replace "" with your password
    properties.put("account", "FR50176");  // replace "" with your account name
    properties.put("db", "MYDATABASE");       // replace "" with target database name
    properties.put("schema", "PUBLIC");   // replace "" with target schema name
    //properties.put("tracing", "on");

    // create a new connection
    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
    // use the default connection string if it is not set in environment
    if(connectStr == null)
    {
     connectStr = "jdbc:snowflake://fr50176.eu-west-2.aws.snowflakecomputing.com"; // replace accountName with your account name
    }
    return DriverManager.getConnection(connectStr, properties);
  }

    @WebMethod
    public List<TableBean> insertRow(@WebParam(name = "arg0") String value) throws Exception
    {
        List<TableBean> tableList =new ArrayList<TableBean>();
      // get connection
      System.out.println("Create JDBC connection");
      Connection connection = getConnection();
      System.out.println("Done creating JDBC connectionn");
      // create statement
      System.out.println("Create JDBC statement");
      Statement statement = connection.createStatement();
      System.out.println("Done creating JDBC statementn");
      // create a table
      System.out.println("Create demo table");
     // statement.executeUpdate("create or replace table OICDemo(C1 STRING)");
      //statement.close();
      System.out.println("Done creating demo tablen");
      // insert a row
      System.out.println("insert into OICDemo values ('"+value+"')");
      statement.executeUpdate("insert into OICDemo values ('"+value+"')");
      //statement.close();
      System.out.println("Done inserting 'hello world'n");
      // query the data
      System.out.println("Query demo");
      ResultSet resultSet = statement.executeQuery("SELECT * FROM OICDemo");
      System.out.println("Metadata:");
      System.out.println("================================");
      // fetch metadata
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      System.out.println("Number of columns=" +
                         resultSetMetaData.getColumnCount());
      for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount();
                           colIdx++)
      {
        System.out.println("Column " + colIdx + ": type=" +
                           resultSetMetaData.getColumnTypeName(colIdx+1));
      }
      // fetch data
      System.out.println("nData:");
      System.out.println("================================");
      int rowIdx = 0;
      while(resultSet.next())
      {
          TableBean bean=new TableBean("rowIdx","0",resultSet.getString(1),"0");
        System.out.println("row " + rowIdx + ", column 0: " +
                           resultSet.getString(1));
          tableList.add(bean);
          rowIdx++;
      }
      statement.close();
      connection.close();
      return tableList;
    }

    @WebMethod
    public List<TableBean> insertRows(@WebParam(name = "Row") List<TableBean> lbean) throws Exception
    {
        List<TableBean> tableList =new ArrayList<TableBean>();
      // get connection
      System.out.println("Create JDBC connection");
      Connection connection = getConnection();
      System.out.println("Done creating JDBC connectionn");
      // create statement
      System.out.println("Create JDBC statement");
      Statement statement = connection.createStatement();
      System.out.println("Done creating JDBC statementn");
     
      // insert a row
     
      for (int i=0;i<lbean.size();i++){
          System.out.println("insert into OICDemo values ("+ "'"+lbean.get(i).getColumn1()+"',"+ "'"+lbean.get(i).getColumn2()+ "',"+ "'"+lbean.get(i).getColumn3()+"',"+  "'"+lbean.get(i).getColumn4()+"')");
      statement.executeUpdate("insert into OICDemo values ("+ "'"+lbean.get(i).getColumn1()+"',"+ "'"+lbean.get(i).getColumn2()+ "',"+ "'"+lbean.get(i).getColumn3()+"',"+  "'"+lbean.get(i).getColumn4()+"')");
      }
      //statement.close();
      System.out.println("Done inserting 'hello world'n");
      // query the data
      System.out.println("Query demo");
      ResultSet resultSet = statement.executeQuery("SELECT * FROM OICDemo");
      System.out.println("Metadata:");
      System.out.println("================================");
      // fetch metadata
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      System.out.println("Number of columns=" +
                         resultSetMetaData.getColumnCount());
      for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount();
                           colIdx++)
      {
        System.out.println("Column " + colIdx + ": type=" +
                           resultSetMetaData.getColumnTypeName(colIdx+1));
      }
      // fetch data
      System.out.println("nData:");
      System.out.println("================================");
      int rowIdx = 0;
      while(resultSet.next())
      {
          TableBean bean=new TableBean(resultSet.getString(1),resultSet.getString(2),resultSet.getString(3),resultSet.getString(4));
        System.out.println("row " + rowIdx + ", column 0: " +
                           resultSet.getString(1));
          tableList.add(bean);
          rowIdx++;
      }
      statement.close();
        connection.close();
      return tableList;
    }

    @WebMethod
    public Boolean setupSnowFlake(@WebParam(name = "tableName") String TableName,
                                 @WebParam(name = "dbName") String DBName,
                                 @WebParam(name = "warehause") String Warehause,
                                 @WebParam(name = "format") String Format,
                                 @WebParam(name = "stageName") String stageName)throws Exception {
        
        
        
        // get connection
        Connection connection = getConnection();
        System.out.println("Done creating JDBC connectionn");
        // create statement
        Statement statement = connection.createStatement();
        System.out.println("Done creating JDBC statementn");

        
        Boolean ret= true;
        
          try {
              
              // create a DB
              statement.executeUpdate("create or replace database " + DBName + ";");             
              System.out.println("Done creating DB " + DBName);            
              
              // create a table
              statement.executeUpdate("create or replace table " + TableName + "(" +
              "id string," +
              "last_name string," +
              "first_name string," +
              "company string," +
              "email string," +
              "workphone string," +
              "cellphone string," +
              "streetaddress string," +
              "city string," +
              "postalcode string);");             
              System.out.println("Done creating table " + TableName);
         
         
              // create a warehouse
              statement.executeUpdate("create or replace warehouse " + Warehause + "with" + 
              " warehouse_size='X-SMALL'" + 
              " auto_suspend = 120" + 
              " auto_resume = true" + 
              " initially_suspended=true;");             
              System.out.println("Done creating warehouse " + Warehause);
              
              // create a format
              statement.executeUpdate(
                            " create or replace file format " + Format +
                            " type = 'CSV'" +
                            " field_delimiter = ','" +
                            " skip_header = 1" +
                            " error_on_column_count_mismatch=false;"  
                  );             
              System.out.println("Done creating format " + Format );
              
              // create a stage
              statement.executeUpdate("create or replace stage " + stageName );             
              System.out.println("Done creating stage " + stageName);
         
          } catch (Exception e) {
              ret = false;
              statement.close();
              connection.close();      
              return ret;
              }      
        //statement.close();
        System.out.println("Setup Completed Successfully");
        
          statement.close();
          connection.close();
          return ret;
    }


    @WebMethod
    public String createTable() throws Exception
    {
       
      // get connection
      System.out.println("Create JDBC connection");
      Connection connection = getConnection();
      System.out.println("Done creating JDBC connectionn");
      // create statement
      System.out.println("Create JDBC statement");
      Statement statement = connection.createStatement();
      System.out.println("Done creating JDBC statementn");
      // create a table
      
      String ret="E";
      System.out.println("Create demo table");
      
        try {
            statement.executeUpdate("create or replace table OICDemo(C1 STRING , C2 STRING, C3 STRING, C4 STRING)");             
        } catch (Exception e) {}      
      //statement.close();
      System.out.println("Done creating demo tablen");
      
        statement.close();
        connection.close();
        
        return ret;
    }

    @WebMethod
    public String uploadService(@WebParam(name = "FilePath") String filePath,
                                @WebParam(name = "InternalFolder") String snowFlakeFolder,
                                @WebParam(name = "FileName") String fileName,
                                @WebParam(name = "StageName") String stageName){
        UploadFileSnowFlake fileSnowFlake=new UploadFileSnowFlake();
        String ret="E";
        try {
             ret= fileSnowFlake.uploadStream( filePath, snowFlakeFolder, fileName, stageName);
        } catch (SQLException e) {
        }
        return ret;
    }

    @WebMethod
    public String copyFileIntoTable(@WebParam(name = "TableName") String tableName,
                                    @WebParam(name = "StageName") String stageName,
                                    @WebParam(name = "OnError") String onError,
                                    @WebParam(name = "FileName") String fileName) throws SQLException {
       
      Connection connection = getConnection();
      Statement statement;
        int result=9;
    String ret="";
        try {
            statement = connection.createStatement();
            result= statement.executeUpdate(" copy into " + tableName + " from @" + stageName +
                                    " FILE_FORMAT = (FORMAT_NAME = mycsvformat) PATTERN='.*" + fileName +
                                    "' ON_ERROR = '"+onError+"'");
            System.out.println(" copy into " + tableName + " from @" + stageName +
                                    " FILE_FORMAT = (FORMAT_NAME = mycsvformat) PATTERN='.*" + fileName +
                                    "' ON_ERROR = '"+onError+"'");
            statement.close();
        } catch (SQLException e) {
            System.out.println("Error "+e.getMessage());
            return "File Processed faild due to error "+e.getMessage();
        }
        //statement.close();
     // String ret=statement.getResultSet().getString(1);
        if(result > 0){
            ret="File Imported successfully with "+result +" rows";
        }else{
            ret="File Import failed";
        }
        try {
            connection.close();
        } catch (SQLException e) {
        }
        return ret;
    }

    @WebMethod
    public String uploadServiceSSH(@WebParam(name = "hostIP") String hostIP, @WebParam(name = "hostPort") int hostPort,
                                   @WebParam(name = "userName") String userName, @WebParam(name = "password") String password,
                                   @WebParam(name = "filePath") String filePath, @WebParam(name = "snowStage") String snowStage,
                                   @WebParam(name = "snowFolder") String snowFolder,
                                   @WebParam(name = "snowFile") String snowFile){
        UploadFileSnowFlake fileSnowFlake=new UploadFileSnowFlake();
        String ret="E";
        try {
             ret= fileSnowFlake.putFileSSH(  hostIP, hostPort,  userName, password, filePath, snowStage, snowFolder, snowFile);
        } catch (SQLException e) {
        } catch (IOException | JSchException | SftpException e) {
        }
        return ret;
    }
}