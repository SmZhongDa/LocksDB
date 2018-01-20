package com.fiberhome.locksdb.jetty;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
 
 public class RmtShellExecutor {
     private static final Logger logger = LoggerFactory.getLogger(RmtShellExecutor.class);
     private Connection conn;
     private String ip;
     private String usr;
     private String psword;
     private String charset = Charset.defaultCharset().toString();
 
     private static final int TIME_OUT = 1000 * 5 * 60;
 
     public RmtShellExecutor(String ip, String usr, String ps) {
         this.ip = ip;
         this.usr = usr;
         this.psword = ps;
     }
 
     private boolean login() throws IOException {
         conn = new Connection(ip);
         conn.connect();
         return conn.authenticateWithPassword(usr, psword);
     }
 
     public String exec(String cmds) throws Exception {
         InputStream stdOut = null;
         InputStream stdErr = null;
         String outStr = "";
         String outErr = "";
         try {
             if (login()) {
                 Session session = conn.openSession();
                 session.execCommand(cmds);
                 stdOut = new StreamGobbler(session.getStdout());
                 outStr = processStream(stdOut, charset);
                 stdErr = new StreamGobbler(session.getStderr());
                 outErr = processStream(stdErr, charset);
                 logger.debug("outErr=" + outErr);
                 session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT);
 
             } else {
            	 logger.error("ssh2 login failure:" + ip);
                 throw new IOException("SSH2_ERR");
             }

         } finally {
             if (conn != null) {
                 conn.close();
             }
             if (stdOut != null)
                 stdOut.close();
             if (stdErr != null)
                 stdErr.close();
         }
 
         return outStr;
     }

     private String processStream(InputStream in, String charset) throws IOException {
         byte[] buf = new byte[1024];
         StringBuilder sb = new StringBuilder();
         while (in.read(buf) != -1) {
             sb.append(new String(buf, charset));
         }
         return sb.toString();
     }
 

 }
