Sahale [![Build Status](https://travis-ci.org/etsy/Sahale.svg)](https://travis-ci.org/etsy/Sahale)
===
A tool to record and visualize metrics captured from Cascading (Scalding) workflows at runtime.

Designed to target the pain points of analysts and end users of Cascading, Sahale provides insight into a workflow's runtime resource usage and makes job debugging and locating relevant Hadoop logs easy. The tool reveals optimization opportunities by exposing inefficient MapReduce jobs in a larger workflow, and enables users to track the execution history of their workflows.

Sahale has been verified to work with several Cascading DSLs, but the example projects and several features are tailored to Twitter's [Scalding DSL](https://github.com/twitter/scalding), which is the DSL we use at Etsy. Pull requests for better support for other flavors of Cascading are welcome!

Installation
===
Step One: Assumes you have a MySQL instance Sahale can use. Clone the Sahale repository and follow instructions in `src/main/sql/create_db_tables.sql` to create the tables the Scala and NodeJS components expect. Modify `db-config.json` in the project's root directory to point to your database.

Step Two: [Install Node Package Manager](https://www.npmjs.com), `cd` into Sahale's root directory, execute `npm install`. Execute `node app` to run the server; browse to localhost on port 5735 and enjoy.

Step Three: [Install Maven3](http://maven.apache.org). Update the `pom.xml` file with the correct Hadoop and Scala/Scalding versions for your Hadoop installation. Update `src/main/resources/flow-tracker.properties` with the hostname you plan to run the NodeJS server on. Keep the port number here in sync with the NodeJS port (see Step Two.) Execute `mvn install`.

User Workflow
===
For a quick test, see `bin/runjob` to run the example job(s). You will need to supply some text file(s) on HDFS to run it against.

Users can run their own tracked Scalding jobs in two ways. Both start by making the Scalding job(s) in question a subclass of `com.etsy.sahale.TrackedJob.`

The easiest way is to add user source code to `src/main/scala/examples`, build the Sahale fatjar with `mvn install`, and execute using `bin/runjob`, just as one would for the included example job. You can add any job dependencies to the fatjar via the `pom.xml`.

The other method is to include the Sahale JAR in your own project build as a dependency, then include it in job runs using `hadoop jar`'s `-libjars` argument. This approach can integrate easily into your existing workflow.

Only jobs submitted to a Hadoop cluster are tracked. No local mode runs are tracked. All tracked jobs must include the argument `--track-job`. The `--track-job` argument is included in the `bin/runjob` convenience script by default.

User who want to track additional job configuration properties using FlowTracker/Sahale, can now add a CSV list of the property names to `flow-tracker.properties` before building the FlowTracker JAR. An example that will add 2 additional data points to each FlowStep update:
`user.selected.configs=mapreduce.task.io.sort.mb,oozie.job.id`

Upgrading
===
Sahale server (NodeJS app) and FlowTracker (Scala client jar) must always maintain parity between clients and server versions. An upgrade deployment must coordinate the distribution of the new client jar and restart of the updated server. In rare cases (tagged in the Git repo) Sahale will make breaking changes that will require addition steps. The two notable cases are listed below:

*Upgrade from 0.7 to 0.8*: The data model has changed. Please recreate your backing MySQL tables using the script in `src/main/sql` before using the new client jar or restarting the NodeJS app. The old tables and data will remain, the new tables will be suffixed with `_new` unless you opt to alias them. No additional change is required.
 
*Upgrade from 0.5 to 0.6*: Mark incompatible changes between older and newer versions of Scala/Scalding. If your org still uses older versions of Scala/Scalding, please see [this](https://github.com/etsy/Sahale/commit/238794f33ba17326a156c396f3dc1dede2b0c743) commit. All other changes and feature upgrades in the 0.6 line will work as expected with this commit reverted and your own choice of versions applied to the `pom.xml`.

The Name
===
Sahale was handmade at [Etsy.com](http://www.etsy.com) and is named for [Sahale Mountain](http://en.wikipedia.org/wiki/Sahale_Mountain), which is a wonderful vantage point from which to view the Cascades

