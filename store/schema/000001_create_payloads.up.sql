CREATE TABLE payloads (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    entity_key VARBINARY(32) NOT NULL,
    payload BLOB NOT NULL,
    content_type VARCHAR(255) NOT NULL DEFAULT '',
    string_attributes MEDIUMTEXT NOT NULL DEFAULT '{}',
    numeric_attributes MEDIUMTEXT NOT NULL DEFAULT '{}'
) ENGINE = InnoDB;