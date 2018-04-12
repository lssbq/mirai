--Drop detail schema
DROP SCHEMA IF EXISTS detail CASCADE;

--Create detail schema after drop
CREATE SCHEMA detail
    AUTHORIZATION lssbq;
COMMENT ON SCHEMA detail
    IS 'The schema to store stocks detail in HS market.';

--Delete from stock_basic
DELETE FROM basic.stock_basic;

--Delete from index basic
DELETE FROM basic.index;

--Delete tables content in meta
DELETE FROM meta.code;
DELETE FROM meta.index;
DELETE FROM meta.daily;
