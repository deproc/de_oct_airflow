CREATE OR REPLACE TRANSIENT TABLE beaconfire_dev_test (name VARCHAR(250), id INT, load_utc_ts datetime);

INSERT INTO beaconfire_dev_test VALUES ('name', 5, sysdate());
