#===========================
# MySQL to Postgres (PG)
#===========================

# Peter R.
# 2022-03-23

#===============================
# Load libraries
#===============================
library(devtools)
#devtools::install_github("eugejoh/pgtools")
library(pgtools)

library(DBI)
library(RMariaDB)
library(dplyr)

# Connect to MySQL database
#connection 1

con_mysql <- dbConnect(RMariaDB::MariaDB(), 
                      host= "localdb",
                      user= "",
                      password= "",
                      db="db1",
                      port= 3306)
                      
# List tables
tL1 <- dbListTables(con_mysql, )

# Create data frame from MySQl tables
my_list <- list()

# Subset tL1 object to work with a sample
#tL1 <- tL1[1:10] 
tL1 <- tL1[c(4,28,34)]

 # loop to get tables/objects
for (i in 1:length(tL1)) {
my_list[[i]]  <- dbGetQuery(con_mysql, paste("SELECT * FROM", tL1[i])) 
}

# create List element names using MySQL table names
for (i in 1:length(tL1)){
names(my_list) <- tL1[1:length(tL1)]
}


# Remove tables with no rows
# create index with df with nrow>0
my_index <- c()

for (i in 1:length(my_list)) {

temp <- if (nrow(my_list[[i]])>0) { i }

my_index <- as.vector(rbind(my_index,temp))

}

# remove df with no rows/records
my_list2 <- my_list[my_index]


# lower case column names
for (i in 1:length(my_list2)) {
names(my_list2[[i]]) <- tolower(names(my_list2[[i]]))

}

# make names db safe: no '.' or other illegal characters,
# all lower case and unique
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

# sanitize column names
for (i in 1:length(my_list2)) {
names(my_list2[[i]]) <- dbSafeNames(names(my_list2[[i]]))

}


# This works 
 con_pg <- DBI::dbConnect(
   drv = RPostgres::Postgres(),
   host = "localhost",
   port = 5434,
   dbname = "gdb1",
   user = "user",
   password = "pw"
 )

 
my_ncharL <- list()

# get sample field lengths to set up a data type
for (i in 1:length(my_list2)) {
# Note new function get_nchar2 as get_nchar does not work well. Run get_nchar2.R first.
my_ncharL[[i]] <- get_nchar2(my_list2[[i]])

my_ncharL[[i]][[1]] <- tolower(my_ncharL[[i]][[1]])

}


# Postgres Field Types   -- Make this into a list!
# Note: pgtools is not working perfectly yet.

for (i in 1:length(my_list2)) {
# Note new function set_pgfields3. Run set_pgfields3.R first 
my_fields <- set_pgfields3(my_list2[[i]], my_ncharL[[i]], conn = con_pg)

# sanitize field names
my_fields <- gsub("\\(-Inf\\)", "", my_fields)
my_fields <- gsub("\\(Inf\\)", "", my_fields)

pg_table <- tolower(names(my_list2[i]))


# Write to Postgres
write_pgtable(
   input = my_list2[[i]],
   tbl_name = pg_table,
   field.types = my_fields,
   conn = con_pg,
   schema="schema",
   clean_vars=TRUE,
   overwrite = FALSE
   )

   
createDate <-Sys.Date()
#downloadTimeStamp <- file.info(path1)$ctime

# add custom comments to PG table
add_pgcomments(con_pg, schema = "schema", tbl_name = pg_table,
  tbl.comments = paste0("Field data (", names(my_list2[i]),") from MySQL db. [Peter R.; ", createDate,"]"), 
  #field.comments = pg_field_comments, 
  verbose = TRUE, overwrite = FALSE) 
   
}

# Add constraints. Add more statments if needed
dbSendStatement(con_pg,  paste0("ALTER TABLE ", "schema",".", names(my_list2[1]), " ADD CONSTRAINT ", "areaid", "_pkey ", "PRIMARY KEY ", "(areaid)"))


# disconnect db connections
dbDisconnect(con_pg)
dbDisconnect(con_mysql)


#===========================
# References
#===========================
# 1) https://www.r-bloggers.com/2016/02/using-postgresql-in-r-a-quick-how-to/
# 2) https://stackoverflow.com/questions/58940624/sqlappendtable-and-dbexecute-work-but-not-dbappendtable-why
