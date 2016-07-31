var elasticsearch = require('elasticsearch');
var async = require('async');
var modify = require('./lib/modify');

var results = [];
var oldIndex = process.argv[2];
var newIndex = process.argv[3];
var doneQuery = false;
var batchSize = process.env.ES_BATCH_SIZE || 200;
var insertSize = process.env.ES_INSERT_SIZE || batchSize * 5
var queryDelay = process.env.ES_QUERY_DELAY || 50;
var orig_queryDelay = queryDelay;
var maxQueue = process.env.ES_MAX_QUEUE || 50000;
var scroll_id;
var handled = 0;
var from = 0;

var client = new elasticsearch.Client({
  host: process.env.ES_HOST || 'localhost:9200',
  log: 'info'
});


function queryBatch(callback) {
  //First run to get the scroll id
  if (!scroll_id) {
    client.search({
      index: oldIndex,
      scroll: '30s',
      search_type: 'scan',
      body: {
        size: batchSize,
        query: {
          "match_all": {}
        }
      }
    }, function(err, resp) {
      console.log(resp);
      if (err) return callback(err);
      scroll_id = resp._scroll_id;
      return callback(null, resp.hits.hits);
    });
  }
  else {
    client.scroll({
      scrollId: scroll_id,
      scroll: '30s'
    }, function(err, resp) {
      if (err) return callback(err);
      if (resp.hits.hits.length === 0)
        doneQuery = true;
      scroll_id = resp._scroll_id;
      return callback(null, resp.hits.hits);
    });
  }
}

function insertBulk(callback) {
  var items = results.splice(0, insertSize);
  var bulk = [];
  items.forEach(function(i) {
    bulk.push({
      index: {
        _index: newIndex,
        _type: i._type,
        _id: i._id
      }
    });
    bulk.push(i._source);
  });
  client.bulk({
    body: bulk
  }, function(err, response) {
    if (err) return callback(err);
    return callback();
  })
}

function runQuery() {
  if (!doneQuery) {
    queryBatch(function(err, hits) {
      var tasks = [];
      if (err) {
        console.log('Query error', err);
        return callback();
      }
      hits.forEach(function(h) {
        tasks.push(function(callback) {
          modify.modifyResult(h, function(err, doc) {
            if (err) return callback(err);
            results.push(doc);
            return callback();
          });
        })
      });
      async.parallel(tasks, function(err) {
        from += batchSize;
        setTimeout(function() {
          runQuery();
        }, queryDelay);
      });
    });
  }
  else
    console.log('COMPLETED QUERYING, WAITING FOR BULKS TO FINISH');
}

function runBulk() {
  if (results.length === 0 && doneQuery) {
    console.log('COMPLETED QUERIES AND BULKS, WAITING FOR 2 SECONDS AND EXITING');
    setTimeout(function() {
      process.exit(0);
    }, 2000);
  }
  else if (results.length === 0) {
    console.log('No items left in queue, pausing for 1 second, consider reducing queryDelay value.');
    setTimeout(function() {
      runBulk();
    }, 1000);
  }
  else {
    insertBulk(function(err) {
      if (err) {
        console.log('INSERT ERR', err);
      }
      else {
        handled += insertSize;
        runBulk();
      }
    });
  }
}

runQuery();
runBulk();

setInterval(function() {
  console.log('Total Handled: ' + handled + ' In memory: ' + results.length);
  //check if we reached maxQueue
  if (results.length >= maxQueue) {
    console.log('Reached maxQueue, delaying queries for 5 seconds.');
    queryDelay = 5000;
  }
  else
    queryDelay = orig_queryDelay;
}, 5000);

process.on('SIGINT', function() {
  console.log("Caught interrupt signal, draining queue");
  doneQuery = true;
});
