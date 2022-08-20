## Creating a Hive Table

# Check the Hive version
hive --version

#login into beeline
beeline -u jdbc:hive2://


# for showing list of databases
show databases;

# creating a database and giving it a name "university"
create database university;

# again showing the list of databases to check wheather university is added or not
show databases;

# from list of databases we are going to use university database
use university;

# There are no tables currently
show tables;

# create a table name "students" and specifying the column name and datatype
create table students(
id bigint,
name string,
department string
);

# showing the table that we have just created
show tables;

# describing the table student i.e. what are the column and the datatypes
describe students;

#inserting value in the column i.e. id, student_name, department (Electronics and communication engineering
#																  Electrical and electronics engineering
#																  Computer science engineering
#																  Mechanical Engineering )



############################################################################################
############################################################################################

## Running Simple Queries

insert into students values(
4126, "Alice", "Engineering"
);

select * from students;

#here are inserting more values for performing some operations on it
insert into students values(
4127, "John", "Journalism"
),(
4129, "Sophia", "Architecture"
),(
4130, "Oliver", "Political Science"
),(
4131, "James", "Engineering"
),(
4132, "Eliza", "Social Work"
),(
4133, "William", "Engineering"
);

#here we want the record of those student who belongs from Mechanical_Engineering department
select * from students where department = "Engineering";

# here we want only the name of the student who belongs from ME department
select name, department from students where department = "Engineering";

# here we want name of the student who are from ME department and their id should be greater than 4130
select * from students where department = "Engineering" and id >4130;

# here we want to print the distinct departments that are present in the records
select DISTINCT department from students;

# here we want the name of the students and their departments but it should be order by department
select name, department from students order by department;

# Total number of records in the table
select count(*) from students;

# We count the number of students in each department
select department, count(*)
from students 
group by department;

# here we are doing the same thing as we did just above but we are giving column name "student_count" to the count values
select department, count(*) as student_count 
from students 
group by department;

#here we are apply limit that we want only 2 records from the table
select * from students limit 2;

# Quit beeline
!quit



############################################################################################
############################################################################################

## Executing Hive Queries from the Shell


# this will execute the command without log in to the beeline and this will login into the beeline execute the command give the output and exit the command line  
beeline -u jdbc:hive2:// \
-e "use university; select * from students"

# we can run a series of command for that we create a text file and write the command we want to execute
# so first create a text file read_students.hql (hql for hive query language)
nano eng_students.hql

# write the command that you want to execute
use university;
select * from students
where department = "Engineering";

#save the file and simply run the below command. Here we are using -f option to specify the script file we want to run
beeline -u jdbc:hive2:// \
-f eng_students.hql


############################################################################################
############################################################################################

## Joining Tables


#login into beeline
beeline -u jdbc:hive2://

# for showing list of databases
show databases;

# from list of databases we are going to use university database
use university;

# create a another table scorecards which has data as record_no, year(2016 & 2017), math_score, english_mark, 
# science_mark, student_id and rank
create table if not exists scorecards (
record_no bigint,
year bigint,
english_score bigint,
physics_score bigint,
calculus_score bigint,
student_id bigint,
category string
);


#show tables
show tables;

#describe scorecards column name and data types
describe scorecards;

# insert values in the table
insert into scorecards values 
(2194, 2016, 97, 94, 89, 4126, "First"),
(2198, 2016, 85, 89, 90, 4130, "Second"),
(2200, 2016, 70, 82, 64, 4133, "Third" ),
(2300, 2017, 99, 96, 92, 4126, "First" ),
(2302, 2017, 76, 91, 90, 4131, "Second" ),
(2303, 2017, 61, 78, 75, 4132, "Third");

# show scorecards table
select * from scorecards;

# join both the tables with the column id that is common in both tables
select students.id, name, category, year, department
from students join scorecards
where students.id = scorecards.student_id;


############################################################################################
############################################################################################

## Exploring the Hive Warehouse


#login into beeline
beeline -u jdbc:hive2://

# here we want to use default databae
use default;

# creating a new database name "university skills"
create database unions;

# we want to show only those databases staring with name "uni"
show databases like 'uni*';

# we have two databases university  and university skills, we want to use university 
use university;

# we want to see the tables that are present in the university  database
show tables;

# Quit Beeline
!quit

# explore the warehouse directory
hadoop fs -ls /user/hive/warehouse


# Check contents of the university.db directory
hadoop fs \
-ls /user/hive/warehouse/university.db


# access into students directory we will get two files here and for both files hive will give some random names 
hadoop fs \
-ls /user/hive/warehouse/university.db/students

# if we will access one of these files we will get the values that we have entered first
hadoop fs \
-cat /user/hive/warehouse/university.db/students/000000_0


# if we access the other file it will show the values that we entered second time
hadoop fs \
-cat /user/hive/warehouse/university.db/students/000000_0_copy_1


# Create another SSH terminal into the master node of the cluster
# Start Beeline
beeline -u jdbc:hive2://

use default;


# create a table name it "professors" and specify the column names and datatypes
create table professors(id int, name string, address string);


# it will show all the tables, previous tables as well as the one which we have created just
show tables;


# navigate to the SSH terminal we opened previously and 
hadoop fs -ls /user/hive/warehouse/


# There are no files to display in the professors directory as the table is empty
hadoop fs -ls /user/hive/warehouse/professors

# Swith to SSH window which is connected to beeline
insert into professors values
(111, 'Cheryl', 'Toronto, ON, Canada');


# show the values of the table
select * from professors;

# now if we again access into the professors directory in the another SSH window it will show the values that we have just entered from the another SSH
hadoop fs -cat /user/hive/warehouse/professors/*

# Insert another row from beeline
insert into professors values
(112, 'Sandra', 'Detroit, MI, USA');

# From the other terminal, check the contents ofthe professors directory
hadoop fs -ls /user/hive/warehouse/professors

# Insert more rows
insert into professors values
(113, 'Fisayo', 'Birmingham, UK'),
(114, 'Anita', 'Fremont, CA, USA');

# Check that one more file has been created in the hadoop FS
hadoop fs -ls /user/hive/warehouse/professors

# now go to beeline SSH window and show the list of tables
show tables;

# from list of tables drop the table named "professors"
drop table professors;


# now if we again show the list of table it will show "professors" table is dropped
show tables;

# Quit beeline
!quit


# again access the warehouse directory we will see no "professors" sub directory is there
hadoop fs -ls /user/hive/warehouse/


############################################################################################
############################################################################################

## External Tables


#login into beeline in one SSH window
beeline -u jdbc:hive2://

# in another SSH window run following commands
# create a file and name it as courses.csv
nano courses.csv

# Enter these values into the file
CS004,Databases,3
CS022,Python,4
CE001,Microprocessors,3

# make a directory and named it as data
hadoop fs -mkdir /data

# copy the csv file that we have created in to this folder
hadoop fs -copyFromLocal courses.csv /data/

# if we access in to the data folder we will see courses.csv file is there
hadoop fs -ls /data/

# go in beeline SSH window
show databases;
use university;
# create a extrenal table and specify the column names as well as datatypes
create external table if not exists courses (
id string,
title string,
credits int 
)
comment "List of courses"
location '/data';

# Confirm that table has been created
describe courses;

# Check the values of the table - all the values are in same column 
select * from courses;

# go to the another SSH window and access university sub directory under the warehouse directory and we will see no subdirectory is present 
hadoop fs -ls /user/hive/warehouse/university.db

# but if we show the list of tables in beeline SSH window subsjects table is there
show tables;

# if we access the data directory from another SSH window courses.csv file is there
hadoop fs -ls /data/

# go to the beeline SSH window and drop the courses table
drop table courses;

# if we again access the data directory from the another SSH window we will see courses.csv file is still there although we drop the table
hadoop fs -ls /data/

# go to beeline SSH window again create table
create external table if not exists courses (
id string,
title string,
credits int
)
comment "List of courses"
row format delimited fields terminated by ','
stored as textfile
location '/data';


# now if we will show the table it will all the values in the right column
select * from courses;

# describe the format of the table students and we will see it is a managed table
describe formatted students;

# if we describe the format of courses table we will see it is a external table
describe formatted courses;


############################################################################################
############################################################################################

## Modifying Tables

#login into beeline in one SSH window
beeline -u jdbc:hive2://

use university;

show tables;


#new subject table schema should be same as the schema of courses
create table if not exists 
advanced_courses like courses;

show tables;

describe advanced_courses;

# change the name of the table advanced_courses to advancedcourses
alter table advanced_courses 
rename to advancedcourses;


#show the list of tables to check whether the name changed or not
show tables;


# add a new column "description" and give the datatype also. It tells about the newly added courses
alter table advancedcourses add columns (
description string
comment "Description of advanced courses"
);


describe advancedcourses;


# alter command can also be used for re-ordering the columns. Here we swap the positions for id and title 
# and rename it as sub_id 
# format should be (original_name, new_name, datatype)
alter table advancedcourses
change column id id string
after title;

describe advancedcourses;

# Switching the column positions does not cause the column data to switch as well
alter table courses
change column id id string
after title;

select * from courses;


############################################################################################
############################################################################################

## Temporary Tables


#login into beeline in one SSH window
beeline -u jdbc:hive2://

use university;

show tables;


# create a temporary table engineering_students and its schema should be same as students table 
create temporary table engineering_students
like students;

show tables;


# if we describe the table we will see it's schema is same as students table
describe engineering_students;

#insert record in temporary table
insert into engineering_students values (
4145,
"Benjamin",
"Civil Engineering"
);

select * from engineering_students;


#quiting the session
!q


#again login to beeline
beeline -u jdbc:hive2://

#show the list of databases
show databases;

# select the database which we are going to use
use university;

# show the list of tables and you will see temporary table engineering_students is not there
show tables;

# create temporary table with the same name as students but mention schema same as scorecards
create temporary table students
like scorecards;

# it will show the temporary table students and hide the original one 
show tables;

#this time it's schema will be same as scorecards schema
describe students; 

# quiting the session
!q


#again login to beeline
beeline -u jdbc:hive2://

show databases;

use university;

# this time schema is same as the previous one
describe students;


############################################################################################
############################################################################################

## Loading Data into Tables


# login beeline in one SSH window
beeline -u jdbc:hive2://

# go to another SSH window and create advancedcourses.csv (subject_id, subject_name, credits)
nano advancedcourses.csv

# writing data in file
EE006,Robotics,4
BE020,Genetics,3
CH002,Thermodynamics,3

# go to beeline SSH window

use university;

describe advancedcourses;

# We swap the positions of the title and id columns
alter table advancedcourses
change column title title string
after id;

describe advancedcourses;

# loading data from local address file name 'advancedcourses.csv' that we have created in another SSH window. 
# Load defines that file is present in local file system not on HDFS
load data local inpath 'advancedcourses.csv'
into table advancedcourses;

# displaying data of advancedcourses table
select * from advancedcourses;

# go to another SSH window and if you explore the warehouse, you will see a copy of the csv file which was used to load the table
hadoop fs -ls \
/user/hive/warehouse/university.db/advancedcourses

# use the cat command to view the contents of the file
hadoop fs -cat \
/user/hive/warehouse/university.db/advancedcourses/advancedcourses.csv

# copy file from local file system to the HDFS "data" directory
hadoop fs -copyFromLocal \
advancedcourses.csv /data/

# checking whether csv file copied or not so displaying list of files present in data directory
hadoop fs -ls /data/

# loading data in advancedcourses table but this time we are not using 'load'
load data inpath '/data/advancedcourses.csv' 
into table advancedcourses;

# displaying data whether it loaded or not
select * from advancedcourses;

# now if you will check university database you will get a copy of csv file
hadoop fs -ls /user/hive/warehouse/university.db/advancedcourses

# but if you check in data directory we will get advancedcourses.csv file is not there that means if you upload file from 
# local file system it will copy the file in the hive warehouse, but if you are not using local then file will move
# from HDFS to hive warehouse
hadoop fs -ls /data/

# we can also use overwrite if we want to zap all the data before loading new data 
load data local inpath 'advancedcourses.csv'
overwrite into table advancedcourses;

# we can also load the data from one table to another
# displaying advancedcourses table data
select * from advancedcourses;

# displaying subject table data
select * from courses;

# inserting advancedcourses data into subjects table
insert into table courses
select id, title, credits from advancedcourses;

# displaying subjects table data and checking whether data inserted from advancedcourses table or not
select * from courses;

# we can use overwrite also if both table have some common data
insert overwrite table courses
select id, title, credits from advancedcourses;

select * from courses;

!quit


############################################################################################
############################################################################################

## Populating Multiple Tables in a Single Query


beeline -u jdbc:hive2://

use university;

describe courses;

select * from courses;

# create a table and name is as course_name. It have two columns (course_id, name), common to courses table. Here we 
# have given column name "name" which is similar to the subject's column name "title" 
create table course_name 
(course_id string, name string);

# create a table and name is as course_credit. It have two columns (course_id, credit), common to courses table
create table course_credit 
(course_id string, credits float);

create table if not exists course_credit 
(course_id string, credits float);

show tables;

# in the first part, from courses table we are overwriting the values of id and title column into course_name's 
# course_id and name column (here overwriting operation will not matter that much as course_name table is empty)
# in the second part, from courses table we are inserting the values of id and credits column into course_credit's 
# course_id and credit column

from courses
insert overwrite table course_name
select id, title
insert into table course_credit
select id, credits;

# showing course_name table data
select * from course_name;

# showing course_credit table data
select * from course_credit;


# we are going to delete the newcourses table
# first we will displaying newsubject table's data
select * from advancedcourses;

# we cannot delete single row or column from the table. Here we are deleting the whole table
truncate table advancedcourses;

# if we again show the newcourses table data, it will show a empty table
select * from advancedcourses;

!quit
