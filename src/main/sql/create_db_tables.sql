-- ...................................................
-- MySQL Table Definition Script:
-- These tables must be created and the DSN
-- information added to db-config.json
-- before you can run the Node server or track jobs.
--
-- If you're upgrading to FlowTracker 0.8.x
-- you should create these tables before upgrading
-- the tracker jar or Node app.
--
-- NOTE: if you need to change the names of these
--       tables, do so at creation time, then set
--       the table name aliases in db-config.json
--       (See the README.md and UPGRADE.md docs)
-- ...................................................

CREATE TABLE `cascading_job_flows_new` (
  `flow_id` char(32) NOT NULL,
  `flow_name` char(96) NOT NULL,
  `flow_status` char(32) NOT NULL,
  `flow_json` text NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  `create_date` int(11) unsigned NOT NULL,
  PRIMARY KEY (`flow_id`)
);

CREATE TABLE `cascading_job_steps_new` (
  `step_id` char(32) NOT NULL,
  `flow_id` char(32) NOT NULL,
  `step_json` text NOT NULL,
  `create_date` int(11) unsigned NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  PRIMARY KEY (`step_id`)
);

CREATE TABLE `cascading_job_edges_new` (
  `flow_id` char(32) NOT NULL,
  `src_stage` int(11) NOT NULL,
  `dest_stage` int(11) NOT NULL,
  `update_date` int(11) unsigned NOT NULL,
  `create_date` int(11) unsigned NOT NULL,
  PRIMARY KEY (`flow_id`, `src_stage`, `dest_stage`)
);

CREATE TABLE `cascading_job_aggregated_new` (
  `flow_id` char(32) NOT NULL,
  `agg_json` text NOT NULL,
  `epoch_ms` bigint(22) unsigned NOT NULL,
  PRIMARY KEY (`epoch_ms`, `flow_id`)
);
