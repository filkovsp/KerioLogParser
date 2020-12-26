# Scripts parsing and analysing Kerio Logs
Currently support "Connection" log.
- ./kerio/Connection.py - modue for parsing Connection Log.
- Files KerioLogAnalyst.* are supposed for analysis.
- KerioLogCollector.py - automation solution for collecting logs from Kerio through JsonRPC API and pushing data into Posgres db.
- KerioLogDBObjects.sql - script for creating tables, views and functions in PSQL