-- Cockroach table
CREATE TABLE sli_revision."13059" (
    RevisionDPId INT PRIMARY KEY,
    Expression varchar(1500) DEFAULT NULL,
    SLIParameterId INT NOT NULL,
    SourceId INT DEFAULT NULL,
    IsImplied INT,
    PeriodStart varchar(64) DEFAULT NULL
);


-- Local mysql

CREATE TABLE `13059` (
    RevisionDPId bigint(20) PRIMARY KEY,
    Expression varchar(1500) DEFAULT NULL,
    SLIParameterId INT NOT NULL,
    SourceId INT DEFAULT NULL,
    IsImplied INT,
    PeriodStart varchar(64) DEFAULT NULL
);

-- py2neo
--