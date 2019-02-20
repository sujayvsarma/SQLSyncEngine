# SQLSyncEngine
Allows automatic synchronization between two SQL Server databases. 

# Pre-requisites for use
The two databases may reside on the same SQL Server or on different SQL Server systems/instances. If they exist on different systems/instances, you should be able to create a LINKED SERVER between the two.

You must deploy the code onto the **destination** SQL Server.

# Instructions for use

1. Create a new database. Can be of any name. Let's use "SQLSyncEngineDB" for this documentation.

2. Deploy the tables, functions and stored procedures (in that order) into the SQLSyncEngineDB database.

3. Edit the table **DataSources**, add one row for every **source database** you wish to sync. You may use FQDN/IP address as per what this SQL Server can use to communicate with the source server.  

4. Edit the table **DataDestinations**, adding one row for every database you wish to pull down. There is no requirement that the names of databases (DatabaseName column) needs to match. You can pull DB1 into DB2.

5. Similarly to #3 and #4 above, edit the table **SourceTables** , adding rows for every **table** on every database you wish to pull down. Remember to match up the right DataSourceId otherwise you will end up with tables in the wrong databases!

    Some pointers about the fields in these tables:
    SyncInsert/SyncUpdate/SyncDelete - set to 1, to enable updating new, modified and deleted rows accordingly.
    
    Insert/Update KeyColumn - Name of the column to consider the key for inserts and updates respectively. For example, an ID column may be the key for detecting a new row, but a LastModified column for a modified row.
    
    Insert/Update Column NULL function - This is currently NOT used, set it to a "" value (NOT NULL!)
    
    Insert/Update Timestamp column - Name of the column that would contain the timestamp on insert and update operations respectively. Example, if you have a seperate Created and LastModified date/time columns, provide those here.
    
    Conflict resolution:
    ConflictHonorSource (SourceTables) - when set to 1, honors data in the source tables (source will overwrite destination)
    ConflictHonorDestination (DestinationTables) - when set to 1, honors data in destination tables (source will not overwrite destination)
    
    When both ConflictHonorSource and ConflictHonorDestination are 1, Source will always win out (source overwrites destination).

6. **TEST** your configuration. Ensure you are logged in to the SQL Server with an account with **sysadmin** permissions. Run the **sp_RunForAllDataSources** stored procedure. This will perform a full sync. Watch the Output window for errors and resolve them. See the **Troubleshooting** tips below for help.

## Automating sync
Create a SQL Server Agent job that runs the sp_RunForAllDataSources at the schedule you want. Don't forget to run this job as **sa**.

# Troubleshooting

1. Did you use an account that has sysadmin privileges to run the stored procedure/job? Easiest is to run as "sa". 

2. Ensure **all** the IsEnabled flags in all the tables are set to 1. If any IsEnabled is 0, then that table or table map will NOT be processed regardless of what value exists in another table. (One zero is enough to disable it).

If that fails, try submitting an issue here with all details you can muster up to help me investigate.
