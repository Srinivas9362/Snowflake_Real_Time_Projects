list @SNOWSTAGE_INTERNAL;

snowstage_internal/bookings.csv.gz
snowstage_internal/hosts.csv.gz
snowstage_internal/listings.csv.gz

;
show tables;

BOOKINGS
HOSTS
LISTINGS
;

COPY INTO BOOKINGS
FRoM @snowstage_internal/bookings.csv.gz;

select * from bookings limit 100;

desc table bookings;


COPY INTO listings
FRoM @snowstage_internal/listings.csv.gz;


COPY INTO hosts
FROM @snowstage_internal/hosts.csv.gz
VALIDATION_MODE = RETURN_ROWS;


COPY INTO hosts
FROM @snowstage_internal/hosts.csv.gz;


