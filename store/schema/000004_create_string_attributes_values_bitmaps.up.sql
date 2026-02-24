CREATE TABLE string_attributes_values_bitmaps (
    name VARCHAR(255) NOT NULL,
    value VARCHAR(1024) NOT NULL,
    bitmap MEDIUMBLOB,
    PRIMARY KEY (name, value)
) ENGINE = InnoDB;