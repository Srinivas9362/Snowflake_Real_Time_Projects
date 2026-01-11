PUT file://D:\Data_Vidya_Projects\Data_Sets\ratings.csv @%raw_ratings;


PUT file://D:\Snowflake_projects\01_Airbnd_Snowflake_DBT\Source_files/*.csv @SNOWSTAGE_INTERNAL;

show stages;
