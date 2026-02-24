CREATE TABLE numeric_attributes_values_bitmaps (
    name VARCHAR(255) NOT NULL,
    value BIGINT NOT NULL,
    bitmap MEDIUMBLOB,
    PRIMARY KEY (name, value)
) ENGINE = InnoDB;