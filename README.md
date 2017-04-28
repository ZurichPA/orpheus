# orpheus

## Connecting to Hive using RJDBC and Kerberos

`orpheus` allows you to execute Hive queries through a JDBC connection to HiveServer2. Working with a Hadoop cluster secured with Kerberos is supported.

`orpheus` gets information about the Hive connection and Kerberos from five enviroment variables.

- `USE_KERBEROS`: set to 1 if using Kerberos
- `HIVE_SERVER_HOST`: Hive server host name
- `HIVE_SERVER_PORT`: Hive server port number
- `HIVE_JAR_FOLDERS`: a list of folders from which to search for jars to add to the classpath when initializing the JVM (in the form `/path/to/folder1:/path/to/folder2:/path/to/folder3`); any jars in the folders will be added to the classpath
- `KERBEROS_REALM`: if you are using Kerberos, the name of the Kerberos realm

