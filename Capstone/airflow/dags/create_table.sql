CREATE TABLE public.dim_us_demographics (
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

CREATE TABLE public.fact_us_immigration (
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

CREATE TABLE public.dim_countries (
  "code" int PRIMARY KEY,
  "name" varchar NOT NULL
);

CREATE TABLE public.dim_us_states (
  "code" varchar PRIMARY KEY,
  "name" varchar
);

CREATE TABLE public.dim_arrival_mode (
  "code" int PRIMARY KEY,
  "mode" varchar NOT NULL
);

CREATE TABLE public.dim_visa (
  "code" int PRIMARY KEY,
  "type" varchar NOT NULL
);

CREATE TABLE public.dim_orig_port (
  "port_code" varchar PRIMARY KEY,
  "name" varchar,
  "st_or_ctry" varchar
);

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("i94addr") REFERENCES "dim_us_demographics" ("state_code");

ALTER TABLE "dim_us_states" ADD FOREIGN KEY ("code") REFERENCES "dim_us_demographics" ("state_code");

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("i94cit") REFERENCES "dim_countries" ("code");

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("i94addr") REFERENCES "dim_us_states" ("code");

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("i94mode") REFERENCES "dim_arrival_mode" ("code");

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("visatype") REFERENCES "dim_visa" ("code");

ALTER TABLE "fact_us_immigration" ADD FOREIGN KEY ("i94port") REFERENCES "dim_orig_port" ("port_code");
