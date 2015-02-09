-- ...................................................
-- MySQL Table Definition Script:
-- These tables must be created and the DSN
-- information added to db-config.json
-- before you can run the Node server or track jobs.
-- ...................................................

CREATE TABLE `cascading_job_flows` (
  `flow_id` char(32) NOT NULL,
  `flow_name` char(96) NOT NULL,
  `flow_status` char(32) NOT NULL,
  `flow_json` text NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  `create_date` int(11) unsigned NOT NULL,
  PRIMARY KEY (`flow_id`)
);

CREATE TABLE `cascading_job_steps` (
  `step_id` char(32) NOT NULL,
  `flow_id` char(32) NOT NULL,
  `step_json` text,
  `create_date` int(11) unsigned NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  PRIMARY KEY (`step_id`)
);

CREATE TABLE `cascading_job_edges` (
  `flow_id` char(32) NOT NULL,
  `src_stage` int(11) NOT NULL,
  `dest_stage` int(11) NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  `create_date` int(11) unsigned NOT NULL
);

