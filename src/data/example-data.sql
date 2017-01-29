CREATE TABLE person (id integer primary key, name text, age integer, city text, school integer);
CREATE TABLE school (id integer primary key, name text, year integer, city text);

INSERT INTO person VALUES (1, 'name1', 21, 'Madrid', 1);
INSERT INTO person VALUES (2, 'name1', 22, 'Madrid', 1);
INSERT INTO person VALUES (3, 'name1', 23, 'Guadalajara', 1);
INSERT INTO person VALUES (4, 'name1', 24, 'Guadalajara', 1);
INSERT INTO person VALUES (5, 'name1', 25, 'Madrid', 2);
INSERT INTO person VALUES (6, 'name1', 26, 'Madrid', 2);
INSERT INTO person VALUES (7, 'name1', 27, 'Madrid', 2);
INSERT INTO person VALUES (8, 'name1', 28, 'Madrid', 3);
INSERT INTO person VALUES (9, 'name1', 29, 'Madrid', 3);
INSERT INTO person VALUES (10, 'name1', 30, 'Madrid', 3);
INSERT INTO person VALUES (11, 'name1', 31, 'Madrid', 4);

INSERT INTO school VALUES (1, 'school1', 1921, 'Madrid');
INSERT INTO school VALUES (2, 'school2', 1922, 'Guadalajara');
INSERT INTO school VALUES (3, 'school3', 1923, 'Guadalajara');
INSERT INTO school VALUES (4, 'school4', 1924, 'Madrid');
