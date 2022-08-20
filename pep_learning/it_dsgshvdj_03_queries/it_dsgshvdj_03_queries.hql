############################################################################################
############################################################################################

## The Array Data Type

#login into beeline
beeline -u jdbc:hive2://

# create a table of laptops which contains data regarding its manufacturing company, name, cost, color and screen size
create table laptops (
company string,
title string,
cost float,
colors array <string>,
screen_size array <float>
);

# show list of tables;
show tables;

# load data in the laptops table
insert into table laptops
select "Dell", "Dell Vostro", 550,
array ("black", "red"), array(float(15.6))
UNION ALL
select "Lenovo", "Lenovo Ideapad", 600,
array ("silver", "black", "white"), array(float(15.6), float(17.3));

# there is an exception thrown by hive, but the operation of loading data is still completed
# this exception is because of CBO that is enalbed by default on any hive. CBO stands for
# Cost Based Optimizer and its an optimization run which ensures that the query that you are
# executing on hive has good performance

select * from laptops;

# from laptops table select name and colors column
select title, colors from laptops;

# select name and first element of colors column
select title, colors[0] from laptops;

# drop laptops table
drop table laptops;

# re create laoptops table and load data from file 
create table laptops (
company string,
title string,
cost float,
colors array<string>,
screen_size array <float>
)
row format delimited fields terminated by ','
collection items terminated by '$';

# switch to another SSH window and make a csv file
nano laptops.csv
ASUS,Asus Vivobook,350,white$silver$black,11.6$15.6
Acer,Acer Aspire,390,black$silver,15.6$17.3

# Confirm the laptops table is empty
select * from laptops;

# load this csv file in the table laptops
load data local inpath 'laptops.csv'
into table laptops;

# showing data of laptops table
select * from laptops;

# showing title, color and screen_size of laptops
select title, colors, screen_size from laptops;

# showing title, and first value of column color and screen_size
select title, colors[0], screen_size[0] from laptops;


############################################################################################
############################################################################################

## The Map Data Type

# droping laptops table
drop table laptops;

# creating table laptops 
create table laptops (
company string,
title string,
cost float,
colors array<string>,
screen_size array<float>,
features map<string, boolean>
)
row format delimited fields terminated by ','
collection items terminated by '$'
map keys terminated by ':';

# describe laptops table (column_name and data_type)
describe laptops;

select * from laptops;

# switch to another SSH window and create csv file
nano laptops_map.csv

ASUS,Asus Vivobook,350,white$silver$black,11.6$15.6,bluetooth:true$finger_print_reader:false
Acer,Acer Aspire,390,black$silver,15.6$17.3,finger_print_reader:false$microphone:true

# switch to beeline SSH window and load csv file
load data local inpath 'laptops_map.csv'
into table laptops;

select * from laptops;

# showing title and features column
select title, features from laptops;

# showing title and features column but this time in features column we are interested in finger_print_reader only
select title, features['finger_print_reader'] from laptops;

# this time we are quering about bluetooth in features column
select title, features['bluetooth'] as bluetooth from laptops;


############################################################################################
############################################################################################

## The Struct Type

# drop table laptops that we created in previous demo
drop table laptops;

# again creating laptops table with some extra info about processor and bluetooth_version
create table laptops (
company string,
title string,
cost float,
colors array<string>,
screen_size array<float>,
features map<string, boolean>,
specs struct<processor:string,bluetooth_version:string>
)
row format delimited fields terminated by ','
collection items terminated by '$'
map keys terminated by ':';

# switch to another SSH window and create csv file 
nano laptops.csv
ASUS,Asus Vivobook,350,white$silver$black,11.6$15.6,bluetooth:true$finger_print_reader:false,Intel Core i3$V4.1
Acer,Acer Aspire,390,black$silver,15.6$17.3,finger_print_reader:false$microphone:true,Intel Core i5

# describing the column name and its datatype
describe laptops;

# load csv file data in the table
load data local inpath 'laptops_struct.csv'
into table laptops;

# showing data for title, features, specs column
select title, features, specs from laptops;

# againing show the same column but in features we are interested in finger_print_reader and in specs column we 
# are interested in bluetooth_version column
select title, features['bluetooth'] as bluetooth_present, 
specs.bluetooth_version from laptops;


############################################################################################
############################################################################################

## The explode and posexplode Functions

# describing column name and datatype
describe laptops;

# showing colors column from laptops
select colors from laptops;

# apply explode function on colors column as it will flatten the color array in every records of this table.
# here we are also assigning a label variants to exploded color column 
select explode(colors) as variants from laptops;

# showing features column from laptops table
select features from laptops;

# exploding the features column and it gives two columns one with all the keys and other with the values. For keys we
# label it as feature and for the corresponding values label it as present
# we are quering for asus company 
select explode(features) as (feature, present) from laptops
where company = "ASUS";

# we are doing same action as we were doing above but this time we are not specifing about the company
select explode(features) as (feature, present) from laptops;

# showing colors column from laptops table
select colors from laptops;

# applying posexplode function on the colors column. This will give to columns one with index values which corresponds to 
# the position of that color in the original array and other with color name and we label it as variants
select posexplode(colors) as (index, variants) from laptops;


# here with explode function we are selecting one more column company as we want to show the company along with color
# this will throw a error because in hive while applying UDTP on a column you can select only same column. So this is a
# limitation
select company, explode(colors) from laptops;

# if we explode color column and apply group by on it so that we can run some summary operation, this will also throw a
# error
select explode(colors) as allcolors from laptops groupby allcolors;


############################################################################################
############################################################################################

## Lateral Views


# describing laptops table (its column name and datatype)
describe laptops;

# exploding colors column and name it as variants and showing this column with title
select title, variants
from laptops
lateral view explode(colors) colorsTable as variants;

# exploding features column and its gives two column, first column name it as feature and name second column as present
select title, feature, present
from laptops
lateral view explode(features) featuresTable as feature, present;

# creating a table and name it as superheroes. In this table, marvel's movie name along with superheroes are there.
create table superheroes (
movie_name string,
superheroes_list array<string>
)
row format delimited fields terminated by ','
collection items terminated by '$';

# switch to another SSH window and create a superheroes.csv file 
nano superheroes.csv
The_Avengers,Iron_man$Thor$Captain_America$Black_Widow$Hawkeye
Avengers:Age_of_Ultron,Iron_man$Vision$Quicksilver$Thor$Captain_America$Black_Widow$Hawkeye
Avengers:Infinity_War,Iron_man$Dr.Strange$Thor$Gamora$Vision$Black_Widow
Spiderman:Homecoming,Iron_man$Spiderman

# switch to beeline SSH window and load the data in superheroes table
load data local inpath 'superheroes.csv'
into table superheroes;

# displaying superheroes data
select * from superheroes;

# exploding superheroes_list column
select explode(superheroes_list) from superheroes;

# exploding superheroes_list and rename output column as superhero and display superhero along with movie_name in table
select movie_name, superhero
from superheroes
lateral view explode(superheroes_list) superheroListTable as superhero;

# we want to count, in how many movies each superhero is there so exploding the superheroes_list column as superhero and
# and group by superhero and then apply count and rename this as column name 'superhero_count'
select superhero, count(*)as superhero_count
from superheroes
lateral view explode(superheroes_list) superheroListTable as superhero
group by superhero;


############################################################################################
############################################################################################

## Multiple Lateral Views

# creating a table customer_orders
create table customer_orders (
names array<string>,
orders array<string>
)
row format delimited fields terminated by ','
collection items terminated by '$';

# switch to another SSH window and create a file customer_orders.csv
nano customer_orders.csv

# write the data in the file and save
Lilly$Helena$Olivia,carrot$mushroom$okra
Charles$Daisy,eggs$arugula
Roxie$Ivy,black_beans$chickpeas

# switch to another SSH window and load csv file in table
load data local inpath 'customer_orders.csv'
into table customer_orders; 

# showing the data of table
select * from customer_orders;

# applying explode function on name column as rename the output column as a
select customer_name, orders
from customer_orders
lateral view explode(names) nameTable as customer_name;

# again we are doing the same thing as we did above but here we are exploding two column names as well as orders.
# we rename the names exploded column as a and orders exploded column as b
select customer_name, order_item
from customer_orders
lateral view explode(names) nameTable as customer_name
lateral view explode(orders) orderTable as order_item;

# creating a table and name it as flatten and it has one column name double nested and it is array of array of string
create table flatten (
double_nested array<array<string>>
);

# inserting data in the table
insert into table flatten
select array(array('mary', 'anna', 'emma'), 
array('sarah', 'ella', 'cora'), 
array('grace', 'helen', 'ethel'));

# showing flatten table
select * from flatten;

# exploding column double nested and showing it as single_nested and againg exploding this single_nested column and
# showing it as name column (here we are showing the whole table)
select * from flatten
lateral view explode(double_nested) nestedTable as single_nested
lateral view explode(single_nested) flatTable as name;

# we are doing the same this but not showing the whole table. we are only showing the single_nested and name column
select single_nested, name from flatten
lateral view explode(double_nested) nestedTable as single_nested
lateral view explode(single_nested) flatTable as name;


############################################################################################
############################################################################################

## Set Operations

load data local inpath 'courses.csv'
overwrite into table courses;

load data local inpath 'advancedcourses.csv'
overwrite into table advancedcourses;

# showing data of courses table
select * from courses;

# showing table of advancedcourses table
select * from advancedcourses;

# selecting all the column of courses tables as well as advancedcourses tables but this is throw an error because 
# advancedcourses table has one extra column that is specilization
select * from courses
union 
select * from advancedcourses;

# so this time we are taking common columns only
select * from courses
union 
select id, title, credits from advancedcourses;

# insert a duplicate value that is already there in one of the table in advancedcourses table 
insert into advancedcourses
values ("EE006","Robotics",4,'5 homeworks, 1 midterm, 1 final');

# if we apply union then in output it will not display the duplicate value
select * from courses
union
select id, title, credits from advancedcourses;

# but if we apply union all then it will display the original value as well as duplicate value 
select * from courses
union all
select id, title, credits from advancedcourses;

select * from (
select id as course_id from courses
union all
select id as course_id from advancedcourses
) allcourses;

select student_id, year from scorecards;
# we join scorecards and students table to find those student id who are in top three rank in 2016,2017
select distinct(student_scores.id) from (
select id from students
join scorecards where
students.id = scorecards.student_id
) student_scores;


############################################################################################
############################################################################################

## The IN and EXISTS clauses

select name from students
where id in
(select student_id from scorecards);

# Those students who do not have their scores recorded in the scorecards table
select name from students
where id not in
(select student_id from scorecards);

select name from students
where exists (
select student_id from scorecards
where scorecards.student_id = students.id);

# not exists
select name from students
where not exists (
select student_id from scorecards
where scorecards.student_id = students.id);


############################################################################################
############################################################################################

## Creating and Populating Tables in One Query

# We try to create a new table from an existing one by specifying the types for the new table
# This results in an error as we cannot specify the types
create table allcourses
(
id string,
title string,
credits float
)
as 
select id, title as name, credits from courses;

# This is the correct way to create a new table from an existing one
create table allcourses
as
select id, title as name, credits from courses;

select * from allcourses;

select * from courses;


############################################################################################
############################################################################################

## Views

# describing column name and datatype of the table students 
describe students;


# describing column name and datatype of the table students
describe scorecards;

# create virtual view table and name it as student_scores
create view student_scores
as
select id, record_no, department
from students join scorecards
where students.id = scorecards.student_id;

# showing the list of the tables and we will see that new table is created and it is there in the list
show tables;

# showing the column name and column's datatype of the table just we have created
describe student_scores;


# describing about the table and in datatype we see it's a type of virtual_view table
describe formatted student_scores;

# showing data of student_results table
select * from student_scores;

# showing distinct id from the table 
select distinct(id) from student_scores;

# inserting values in the table but this will throw a error as we can't insert values in virtual view 
# table
insert into student_scores values (4139, 2199, "Biology");

create view english_scores
as
select record_no, english_score
from scorecards;

# describing scorecard_subjects column name and datatype
describe english_scores;

# displaying the data of the table 
select * from english_scores;

# We include the student id in the table
alter view english_scores
as
select record_no, english_score, student_id
from scorecards;

select * from english_scores;