#===========================
# SPSS to Postgres (PG)
#===========================

# Peter R.
# 2022-03-23

#===========================
# Folder setup
#===========================

path1 <- "path"
file1 <- "file.sav"

# PG object names
pgT1 <- "abc4_survey_test6"  # Table name
pgSc1 <- "public"   # Schema name


#===========================
# Libraries
#===========================
library(haven)
library(DBI)


#===========================
# Read SPSS (.sav)
#===========================
df1 <- read_sav(paste0(path1, file1))

# make names db safe: no '.' or other illegal characters,
# all lower case and unique
dbSafeNames = function(names) {
  names = gsub('[^a-z0-9]+','_',tolower(names))
  names = make.names(names, unique=TRUE, allow_=TRUE)
  names = gsub('.','_',names, fixed=TRUE)
  names
}

colnames(df1) = dbSafeNames(colnames(df1))


#===========================
# Send dataframe to PG
#===========================

# Note: First create empty PG table with constraints using pgAdmin

# Create a connection to the database
pg <- dbDriver("PostgreSQL")


# add user and password
con1 <- dbConnect(RPostgres::Postgres(), user="", password="",
                 host="localhost", port=5434, dbname="db1")

dbWriteTable(con1,Id(schema=pgSc1, table=pgT1),df1, overwrite=F, append=T, row.names=FALSE)

dbSendStatement(con1, paste0("COMMENT ON TABLE ", pgSc1,".", pgT1, " IS 'Abc123 data. Created on 2022-02-23.'"))


# disconnect from the database
dbDisconnect(con1)


#===========================
# References
#===========================
# 1) https://www.r-bloggers.com/2016/02/using-postgresql-in-r-a-quick-how-to/
# 2) https://stackoverflow.com/questions/58940624/sqlappendtable-and-dbexecute-work-but-not-dbappendtable-why


