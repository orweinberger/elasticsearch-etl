var elasticsearch = require('elasticsearch');
var async = require('async');

var results = [];
var oldIndex = process.argv[2];
var newIndex = process.argv[3];
var doneQuery = false;
var batchSize = ES_BATCH_SIZE || 500;
var scroll_id;
var handled = 0;
var from = 0;

var client = new elasticsearch.Client({
  host: ES_HOST || 'localhost:9200',
  log: 'info'
});


function queryBatch(callback) {
  //First run to get the scroll id
  if (!scroll_id) {
    client.search({
      index: oldIndex,
      scroll: '1m',
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
  } else {
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

function modifyResult(document, callback) {
  return callback(null, document);
}

function insertBulk(callback) {
  var items = results.splice(0, batchSize);
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
    console.log('RUNNING QUERY');
    queryBatch(function(err, hits) {
      var tasks = [];
      if (err) {
        console.log('Query error', err);
        return callback();
      }
      hits.forEach(function(h) {
        tasks.push(function(callback) {
          modifyResult(h, function(err, doc) {
            if (err) return callback(err);
            results.push(doc);
            return callback();
          });
        })
      });
      async.parallel(tasks, function(err) {
        from += batchSize;
        console.log('Fetched batch, result array size is', results.length);
        runQuery();
      });
    });
  } else
    console.log('COMPLETED QUERYING, WAITING FOR BULKS TO FINISH');
}

function runBulk() {
  console.log('RUNNING BULK');
  if (results.length === 0 && doneQuery) {
    console.log('COMPLETED QUERIES AND BULKS, WAITING FOR 2 SECONDS AND EXITING');
    setTimeout(function() {
      process.exit(0);
    }, 2000);
  } else if (results.length === 0) {
    console.log('NO ITEMS TO PUSH YET, PAUSING FOR 1 SECOND');
    setTimeout(function() {
      runBulk();
    }, 1000);
  } else {
    insertBulk(function(err) {
      if (err) {
        console.log('INSERT ERR', err);
      } else {
        console.log('Inserted batch, result array size is', results.length, ' Total inserted:', handled);
        runBulk();
      }
    });
  }
}

runQuery();
runBulk();
