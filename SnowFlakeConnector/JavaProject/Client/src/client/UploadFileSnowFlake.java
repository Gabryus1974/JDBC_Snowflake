package client;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.UserInfo;

import com.trilead.ssh2.SFTPException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.file.Path;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.Properties;

import net.snowflake.client.jdbc.SnowflakeConnection;

import org.apache.commons.io.IOUtils;

public class UploadFileSnowFlake {
    public UploadFileSnowFlake() {
        super();
    }
    public String uploadStream(String filePath,String snowFlakeFolder,String fileName,String stageName)
    throws SQLException{
        Connection connection =getConnection();
        File file = new File(filePath);
        FileInputStream fileInputStream =null;
        String ret="E";
        try {
            fileInputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            System.out.println("Error "+e.getMessage());
            return "File Upload faild due to error "+e.getMessage();
            
        }

        // upload file stream to user stage
      /*  connection.unwrap(SnowflakeConnection.class).uploadStream("OICSTAGE", "testUploadStream",
           fileInputStream, "destFile.csv", true);*/
      connection.unwrap(SnowflakeConnection.class).uploadStream(stageName, snowFlakeFolder,
                 fileInputStream, fileName, true);
        ret= "File Successfully Uploaded";
        return ret;
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
    public  String putFileSSH(String hostIP,int hostPort, String userName,String password,String filePath,String snowStage,String snowFolder,String snowFile) throws JSchException, SftpException, SQLException, IOException {
        JSch jsch = new JSch();
            Session session = null;
        InputStream f=null;
        String ret="";
            try {
                session = jsch.getSession(userName, hostIP, hostPort);
                session.setConfig("StrictHostKeyChecking", "no");
                session.setPassword(password);
                session.connect();

                Channel channel = session.openChannel("sftp");
                channel.connect();
                ChannelSftp sftpChannel = (ChannelSftp) channel;
                f=sftpChannel.get(filePath);
                System.out.println( "Phase one ");
                try {
                    UploadFileSnowFlake up=new UploadFileSnowFlake();
              
                    Connection connection =getConnection();
                    File file = up.stream2file(f);
                    FileInputStream fileInputStream =null;
                    
                    try {
                        fileInputStream = new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        System.out.println("Error "+e.getMessage());
                        System.out.println( "File Upload faild due to error "+e.getMessage());
                        ret="File Upload faild due to error "+e.getMessage();
                        
                    }

                    // upload file stream to user stage
                      connection.unwrap(SnowflakeConnection.class).uploadStream(snowStage, snowFolder,
                       fileInputStream, snowFile, true);
                  
                    ret= "File Successfully Uploaded";
                    System.out.println( ret + "--" + hostIP + "--"+  hostPort+ "--"  +  userName+ "--" + password+ "--" + filePath+ "--" + snowStage+ "--" +  snowFolder+ "--" + snowFile);
                    
                    
                } catch (Exception e) {
                    System.out.println("Error "+e.getMessage());
                }
                sftpChannel.exit();
                session.disconnect();
            } catch (JSchException e) {
                e.printStackTrace();  
                ret="File Upload faild due to error " +e.getMessage();
            } catch (SftpException e) {
                e.printStackTrace();
                ret="File Upload faild due to error "+e.getMessage();
            }
            return ret;
    }
    public static void main (String [] args) throws IOException, SQLException {
        try {
            UploadFileSnowFlake up=new UploadFileSnowFlake();
            String i=up.putFileSSH( "IP", 5024,"","Password","","OICSTAGE","testUploadStream","destFile.csv");
          
            
        } catch (JSchException | SftpException e) {
            System.out.println("Error "+e.getMessage());
        }
    }
    public static File stream2file (InputStream in) throws IOException {
           final File tempFile = File.createTempFile("TempFile", ".csv");
           tempFile.deleteOnExit();
           try (FileOutputStream out = new FileOutputStream(tempFile)) {
               IOUtils.copy(in, out);
           }
           return tempFile;
       }
}
