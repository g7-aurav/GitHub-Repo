-- Run this query before setting up SynapseServerless Linkedin Service
-- In SynapseServerless Linkedin Service use ReadExternalDataDB as database

CREATE DATABASE ReadExternalDataDB; 

CREATE EXTERNAL DATA SOURCE FintechDataExternal
WITH (
    LOCATION = 'https://<your_storage_account>.dfs.core.windows.net/<container_name>'
);