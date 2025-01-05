-- Lab3-查询执行 测试点5：连接查询
create table student (id int, name char(9), major char(32));
create table grade (course char(9), student_id int, score float);
insert into student values (1, 'TomTomTom', 'Computer ScienceComputer Science');
insert into student values (2, 'JerryJerr', 'Computer ScienceComputer Science');
insert into student values (3, 'JackJackJ', 'Electrical Engineeringer Science');
insert into grade values ('DataDataa', 1, 90.0);
insert into grade values ('DataDataa', 2, 91.25);
insert into grade values ('DataDataa', 3, 90.0);
insert into grade values ('DataDataa', 4, 82.0);
insert into grade values ('DataDatab', 4, 85.0);
insert into grade values ('DataDatac', 4, 85.0);
insert into grade values ('DataDatad', 4, 90.5);
select * from student, grade;
select id, name, major, course, score from student, grade where student.id = grade.student_id;
