# MS SQL Database Duplicator 
A tool to make a duplicate copy of a Microsoft SQL Server database within the same server.

### How to build and run.

Clean and make a Eclipse project.
`$ mvn clean eclipse:eclipse -U`

Make a single jar with embedded dependencies.
`$ mvn clean compile assembly:single`

Run.
`$ java -jar target/mssqlduplicator-0.1-jar-with-dependencies -c creds.properties -s localhost -d "TestDB" -n "CopyDB" --overwrite

See the sample.creds.properties file for an example of how the credential file should like.