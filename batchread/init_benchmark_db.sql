-- creates a DB of 350 million rows
-- resulting table size is 20 GB
-- took about 15 minutes on my machine

CREATE TABLE public.random_read_test AS
SELECT lpad(num::text,20,'0') AS PK, rpad(num::text||' - ',33,'*') AS payload
FROM (SELECT * FROM generate_series(1,350000000)) t (num);
-- create the primary index of 13 GB size.
-- took about 5 minutes on my machine

ALTER TABLE public.random_read_test
    ADD CONSTRAINT random_read_test_pk PRIMARY KEY (pk);
