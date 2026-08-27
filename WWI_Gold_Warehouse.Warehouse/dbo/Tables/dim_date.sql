CREATE TABLE [dbo].[dim_date] (

	[date_key] int NOT NULL, 
	[full_date] date NOT NULL, 
	[day_number] int NOT NULL, 
	[day_name] varchar(10) NOT NULL, 
	[day_of_week] int NOT NULL, 
	[day_of_year] int NOT NULL, 
	[week_of_year] int NOT NULL, 
	[month_number] int NOT NULL, 
	[month_name] varchar(10) NOT NULL, 
	[month_short] varchar(3) NOT NULL, 
	[quarter_number] int NOT NULL, 
	[quarter_name] varchar(2) NOT NULL, 
	[year_number] int NOT NULL, 
	[fiscal_year] int NOT NULL, 
	[is_weekend] bit NOT NULL, 
	[is_holiday] bit NOT NULL
);