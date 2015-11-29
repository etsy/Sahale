
// the app's dependencies
var express = require('express')
  , routes = require('./routes')
  , http = require('http')
  , path = require('path')
  , bodyParser = require('body-parser')
  , favicon = require('serve-favicon')
  , logger = require('morgan')
  , methodOverride = require('method-override');

// customize this to taste
var listen_port = 5735;

var app = express();

app.set('port', listen_port);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.disable('etag');
app.use(favicon(__dirname + '/public/images/favicon.ico'));
app.use(logger('dev'));
app.use(bodyParser.urlencoded({ extended: true, limit: '5mb' }));
app.use(bodyParser.json({ extended: true, limit: '5mb' }));
app.use(methodOverride('_method'));
app.use(require('stylus').middleware(__dirname + '/public'));
app.use(express.static(path.join(__dirname, 'public')));

if (app.get('env') == 'development') {
  app.locals.pretty = true;
}

// Views
app.get('/', routes.index);
app.get('/help', routes.help);
app.get('/flowgraph/:flow_id', routes.flowgraph);
app.get('/history/:flow_name', routes.history);
app.get('/load/:cluster', routes.load);
app.get('/search', routes.search);

// JSON API for db reads
app.get('/flows/running', routes.flows_running);
app.get('/flows/completed', routes.flows_completed);
app.get('/flows/completed/all', routes.flows_completed_all);
app.get('/flows/search/:searchterm', routes.flow_search);
app.get('/flow/:flow_id', routes.flow);
app.get('/steps/:flow_id', routes.steps);
app.get('/edges/:flow_id', routes.edges);
app.get('/flow_history/:flow_name', routes.flow_history);
app.post('/step_group', routes.step_group); // read only but uses POST due to param size
app.get('/sahale_config_data', routes.sahale_config_data);

// JSON API routes & handlers for POSTed data from running Cascading jobs
app.post('/flow/update/:flow_id', routes.insert_or_update_flow);
app.post('/steps/update/:flow_id', routes.insert_or_update_steps);
app.post('/edges/update/:flow_id', routes.insert_or_update_edge);

process.title = "sahale" // for output in "ps"
http.createServer(app).listen(app.get('port'), function() {
  console.log("Cascading Job Visualizer has started successfully.");
  console.log("Express server listening on port " + app.get('port'));
});
