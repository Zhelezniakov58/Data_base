drop database if exists assigmet_2;
create database assigmet_2;
use assigmet_2;
set global local_infile = 1;
SET SESSION max_execution_time = 1200;

create table users(
id int, 
display_name text, 
views int, 
reputation int, 
location text
);

create table comments(
id int,
user_id int, 
post_id int, 
creation_date timestamp
);

create table posts_questions(
owner_user_id int, 
id int, 
title text, 
last_edit_date timestamp
);

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/comments.csv' INTO TABLE comments
fields terminated by ","
ignore 1 rows;

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/users.csv' INTO TABLE users
fields terminated by ",";

LOAD DATA LOCAL INFILE '/Users/sasha0910/Programming/MySQL/posts_questions.csv' INTO TABLE posts_questions
fields terminated by ","
ignore 1 rows;

-- code writed by AI
explain analyze

SELECT 
    u.display_name,
    u.location,
    u.reputation,
    c.creation_date AS comment_date,
    p.title AS question_title,
    p.last_edit_date
FROM (
    SELECT * 
    FROM users 
    WHERE reputation >= 0
      AND location IS NOT NULL
) AS u
JOIN (
    SELECT 
        user_id,
        post_id,
        creation_date
    FROM comments
    WHERE creation_date IS NOT NULL
) AS c
    ON u.id = c.user_id
JOIN (
    SELECT 
        id,
        owner_user_id,
        title,
        last_edit_date
    FROM posts_questions
    WHERE last_edit_date IS NOT NULL
) AS p
    ON u.id = p.owner_user_id
WHERE 
    u.id IN (SELECT user_id FROM comments WHERE creation_date IS NOT NULL)
    AND u.id IN (SELECT owner_user_id FROM posts_questions WHERE last_edit_date IS NOT NULL)
    AND (u.reputation > 100 OR u.views > 500)
    AND (p.last_edit_date > c.creation_date OR p.last_edit_date IS NOT NULL)
    AND u.location <> ''
    AND p.title IS NOT NULL
    AND c.post_id IS NOT NULL;
    
-- My code 
create index users_index on users(id, reputation, views);
create index posts_questions_index on posts_questions(owner_user_id);
create index comments_index on comments(user_id);

explain analyze
with posts_questions_cte as(
SELECT id, owner_user_id, title, last_edit_date
FROM posts_questions
WHERE last_edit_date IS NOT NULL
),
comments_cte as(
SELECT 
user_id,
post_id,
creation_date
FROM comments
WHERE creation_date IS NOT NULL
),
users_cte as(
SELECT * 
FROM users 
where location IS NOT NULL
and reputation >= 0
)
SELECT  
    u.display_name,
    u.location,
    u.reputation,
    c.creation_date AS comment_date,
    p.title AS question_title,
    p.last_edit_date
FROM users_cte AS u
JOIN comments_cte AS c 
	ON u.id = c.user_id
JOIN posts_questions_cte AS p
	ON u.id = p.owner_user_id
WHERE 
    (u.reputation > 100 OR u.views > 500)
    AND p.last_edit_date > c.creation_date
    AND u.location <> '';
-- with optimizer hint
explain analyze
with posts_questions_cte as(
SELECT id, owner_user_id, title, last_edit_date
FROM posts_questions
WHERE last_edit_date IS NOT NULL
),
comments_cte as(
SELECT 
user_id,
post_id,
creation_date
FROM comments
WHERE creation_date IS NOT NULL
),
users_cte as(
SELECT * 
FROM users IGNORE INDEX (users_index)
where location IS NOT NULL
and reputation >= 0
)
SELECT  
    u.display_name,
    u.location,
    u.reputation,
    c.creation_date AS comment_date,
    p.title AS question_title,
    p.last_edit_date
FROM users_cte as u 
JOIN comments_cte AS c 
	ON u.id = c.user_id
JOIN posts_questions_cte AS p
	ON u.id = p.owner_user_id
WHERE 
    (u.reputation > 100 OR u.views > 500)
    AND p.last_edit_date > c.creation_date
    AND u.location <> '';



