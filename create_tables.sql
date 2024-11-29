CREATE TABLE "race_dim" (
  "raceId" int PRIMARY KEY,
  "circuitId" int,
  "year" int,
  "round" int,
  "name_x" varchar,
  "date" date,
  "time_races" time,
  "url_x" varchar
  
);

CREATE TABLE "circuit_dim" (
  "circuitId" int PRIMARY KEY,
  "circuitRef" varchar,
  "name_y" varchar,
  "url_y" varchar,
  "location" varchar,
  "country" varchar,
  "lat" float,
  "lng" float,
  "alt" int
);

CREATE TABLE "session_dim" (
  "raceId" int,
  "fp1_date" date,
  "fp1_time" time,
  "fp2_date" date,
  "fp2_time" time,
  "fp3_date" date,
  "fp3_time" time,
  "quali_date" date,
  "quali_time" time,
  "sprint_date" date,
  "sprint_time" time
);

CREATE TABLE "constructor_standings_dim" (
  "constructorStandingsId" int PRIMARY KEY,
  "constructorId" int,
  "raceId" int,
  "points_constructorstandings" float,
  "wins_constructorstandings" int,
  "position_constructorstandings" int
);

CREATE TABLE "driver_standings_dim" (
  "driverStandingsId" int PRIMARY KEY,
  "driverId" int,
  "raceId" int,
  "points_driverstandings" float,
  "wins" int,
  "position_driverstandings" int
);

CREATE TABLE "driver_dim" (
  "driverId" int PRIMARY KEY,
  "driverRef" varchar,
  "forename" varchar,
  "surname" varchar,
  "dob" date,
  "code" varchar,
  "url" varchar,
  "number" int,
  "nationality" varchar,
  "number_drivers" int
);

CREATE TABLE "constructor_dim" (
  "constructorId" int PRIMARY KEY,
  "constructorRef" varchar,
  "name" varchar,
  "nationality_constructors" varchar,
  "url_constructors" varchar
);

CREATE TABLE "status_dim" (
  "statusId" int PRIMARY KEY,
  "status" varchar
);

CREATE TABLE "lap_dim" (
  "lap" int,
  "raceId" int,
  "driverId" int,
  "position_laptimes" int,
  "time_laptimes" time,
  "milliseconds_laptimes" int
);

CREATE TABLE "stop_dim" (
  "stop" int,
  "raceId" int,
  "driverId" int,
  "lap_pitstops" int,
  "time_pitstops" time,
  "duration" float,
  "milliseconds_pitstops" int
);

CREATE TABLE "result_fact" (
  "resultId" int PRIMARY KEY,
  "driverId" int,
  "constructorId" int,
  "raceId" int,
  "statusId" int,
  "driverStandingsId" int,
  "constructorStandingsId" int,
  "grid" int,
  "positionOrder" int,
  "points" float,
  "time" varchar,
  "rank" int,
  "milliseconds" int,
  "fastestLapSpeed" float,
  "fastestLapTime" time,
  "fastestLap" int,
  "laps" int
);

ALTER TABLE "race_dim" ADD FOREIGN KEY ("circuitId") REFERENCES "circuit_dim" ("circuitId");

ALTER TABLE "session_dim" ADD FOREIGN KEY ("raceId") REFERENCES "race_dim" ("raceId");

ALTER TABLE "lap_dim" ADD FOREIGN KEY ("raceId") REFERENCES "race_dim" ("raceId");

ALTER TABLE "lap_dim" ADD FOREIGN KEY ("driverId") REFERENCES "driver_dim" ("driverId");

ALTER TABLE "stop_dim" ADD FOREIGN KEY ("raceId") REFERENCES "race_dim" ("raceId");

ALTER TABLE "stop_dim" ADD FOREIGN KEY ("driverId") REFERENCES "driver_dim" ("driverId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("driverId") REFERENCES "driver_dim" ("driverId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("constructorId") REFERENCES "constructor_dim" ("constructorId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("raceId") REFERENCES "race_dim" ("raceId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("statusId") REFERENCES "status_dim" ("statusId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("driverStandingsId") REFERENCES "driver_standings_dim" ("driverStandingsId");

ALTER TABLE "result_fact" ADD FOREIGN KEY ("constructorStandingsId") REFERENCES "constructor_standings_dim" ("constructorStandingsId");
