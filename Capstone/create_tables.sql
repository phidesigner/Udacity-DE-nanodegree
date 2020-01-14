CREATE TABLE "dim_us_demographics" (
  "city" varchar,
  "median_age" int,
  "male_population" int,
  "female_population" int,
  "total_population" int,
  "number_of_veterans" int,
  "foreign_born" int,
  "average_household_size" int,
  "state_code" varchar PRIMARY KEY,
  "race" varchar,
  "count" int NOT NULL
);

CREATE TABLE "fact_immigration" (
  "cicid" int PRIMARY KEY NOT NULL,
  "arrdate" date,
  "depdate" date,
  "i94cit" int,
  "i94yr" int,
  "i94mon" int,
  "i94res" int,
  "i94port" varchar,
  "i94mode" int,
  "i94addr" varchar,
  "i94bir" int,
  "i94visa" int,
  "dtadfile" varchar,
  "visapost" varchar,
  "occup" varchar,
  "entdepa" varchar,
  "entdepd" varchar,
  "matflag" varchar,
  "biryear" int,
  "dtaddto" varchar,
  "gender" varchar,
  "airline" varchar,
  "admnum" int,
  "fltno" varchar,
  "visatype" varchar
);

CREATE TABLE "dim_countries" (
  "code" int PRIMARY KEY,
  "name" varchar NOT NULL
);

CREATE TABLE "dim_us_states" (
  "city_code" varchar PRIMARY KEY,
  "name" varchar,
  "state_code" varchar,
  "state_name" varchar
);

CREATE TABLE "dim_arrival_mode" (
  "code" int PRIMARY KEY,
  "name" varchar NOT NULL
);

CREATE TABLE "dim_visa" (
  "code" int PRIMARY KEY,
  "type" varchar NOT NULL
);

ALTER TABLE "fact_immigration" ADD FOREIGN KEY ("i94addr") REFERENCES "dim_us_demographics" ("state_code");

ALTER TABLE "dim_us_states" ADD FOREIGN KEY ("state_code") REFERENCES "dim_us_demographics" ("state_code");

ALTER TABLE "fact_immigration" ADD FOREIGN KEY ("i94cit") REFERENCES "dim_countries" ("code");

ALTER TABLE "fact_immigration" ADD FOREIGN KEY ("i94port") REFERENCES "dim_us_states" ("city_code");

ALTER TABLE "fact_immigration" ADD FOREIGN KEY ("i94mode") REFERENCES "dim_arrival_mode" ("code");

ALTER TABLE "fact_immigration" ADD FOREIGN KEY ("visatype") REFERENCES "dim_visa" ("code");
