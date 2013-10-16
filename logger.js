/*
S3AccessLogParser:
each_web: {
  domain: 
  count: Number,
  fileAccess : {[
    count:
    file: 
    dateAcc:
  ]}

  TODO (opt): CHANGE THE PSEUDO LOGS TO MATCH THE NEW ONES ON S3 THAT YOU SEE
  TODO (opt): Allow more flexibility by specifying how to parse, 
              i.e. a string ("date ip-thedomain something something ... ") and parse based on that
*/

//dependency and modules
var async    = require('async')
  , fs       = require('fs')
  , request  = require('request')
  , zlib     = require('zlib')
  , knox     = require('knox')
  , moment   = require('moment')
  , connect  = require('connect')
  , express  = require('express')
  , io       = require('socket.io')
  , port     = (process.env.PORT || 8081);

//Setup Express - uncomment the below if you want to examine the logs for bugs
var server = express.createServer();
server.configure(function(){
    server.set('views', __dirname + '/views');
    server.set('view options', { layout: false });
    server.use(connect.bodyParser());
    server.use(express.cookieParser());
    server.use(express.session({ secret: "shhhhhhhhh!"}));
    server.use(connect.static(__dirname + '/static'));
    server.use(server.router);
});

//Setup Socket.IO and server
/*
//server.listen( port);
var io = io.listen(server);
io.sockets.on('connection', function(socket){
  console.log('Client Connected');
  socket.on('message', function(data){
    socket.broadcast.emit('server_message',data);
    socket.emit('server_message',data);
  });
  socket.on('disconnect', function(){
    console.log('Client Disconnected.');
  });
});*/

//fake urls and file extensions to simulate access logs
var urls = [
    "ottp.webflow", 
    "bryant", 
    "afjaldasd", 
    "Asdfljlasdf", 
    "cool.webflow", 
    "superduper",
    "uau", 
    "somestuff",
    "wee",
    "blahbalbh.webflow",
    "woooooootss", 
    "random.webflow",
    "ost.webflow",
    "pleeas"
];

var file_ext = [
    ".css",
    ".html",
    ".js"
]; 

//take this out?
var count = 0;

//stuff to be set

var allLogs = []; //pushed each log
var allWebs = []; //everything that goes into mongoose

//what gets pushed out from the module
var finalObject = {};
///////////////////////////////////////////
//              Routes                   //
///////////////////////////////////////////

/**
  Create a random number of pseudos less than then number specified by specifying the random option
  Or just write out a bunch according to the number specified
 */
server.get('/pseudo/:number/:option', function(req,res){
  var num = 0;
  if(option === "random") 
    num = Math.round(Math.random()*req.params.number);
  else 
    num = req.params.number;
  for(var i = 0; i < num; i++)
    res.write(createOnePseudo() + "\n");
  res.end();
});

/**
  Write out a number of pseudos to a file called data.txt which you can read from and parse
*/
server.get('/write/:num', function(req, res) {
  var file = __dirname + '/data.txt';
  var data = "";
  for(var i = 0; i < req.params.num; i++)
    data += createOnePseudo() +"\n";
  fs.writeFile(file, data, function(err) {
    if(err)
    {  
      console.log("ERROR: " + err);
    } else {
      console.log("saved"); 
      res.end("saved");
    }
  });
});

/**
  PREREQ- Must have done a /write/:num request to write to data.txt
  This one tests it out and spits out the allWebs object which contains everything
  It also logs to terminal how many counts there were so you can verify it
  */
server.get('/test', function(req, res) {
  var readable = fs.createReadStream('data.txt');
  var first = "";
  var second = ""; //the second part that gets delayed to the next half 
  var everything="";
  readable.pipe(new MyStream()).pipe(res);
});

//just view the object allWebs
server.get('/printall', function(req, res) {
  res.end(JSON.stringify(allWebs, null, '\t'));
});

/**
  HELPER FUNCTIONS! 
    1) createOnePseudo: create a log in between two dates and just write it out
    2) mainProc: works on the "everything" object -> separates -> parses each -> spit out result
    3) parseAll- used by mainProc to parse each chunk separated in allLogs
    4) randomDate- spits out a random Date between 2 javascript date objects
    5) putInArr- splits by the bracket and puts it into arrays
 **/
function createOnePseudo() {
  var rd = randomDate(new Date(2012, 0, 1), new Date(2030, 1, 1)).toUTCString();
  var arr = rd.split(' ');
  //[1] = day of month
  //[2] = month
  //[3] = year
  //[4] = time hour:min:sec
  // assume all is +0000
  var log = "[" + arr[1] + "/" +arr[2] + "/"+arr[3] + ":" + arr[4] + " +0000] 10.154.175.110 - - - ";
  
  //take a random url and add .com
  var url = urls[Math.round(Math.random()*(urls.length-1))];
  var fixed_url = url+".com ";
  log += fixed_url;
  
  var file ="/";
  var web = "";
  if(url.indexOf(".webflow") !== -1)  //if contains webflow keyword
  {
     web = url.substring(0, url.indexOf(".webflow"));
     file += web;
  } else {
     file += url;
     web  = url;
  }
  //extract file extension and put it in
  file +=  file_ext[Math.round(Math.random()*(file_ext.length-1))];

  log += (file + "  to: -: GET " + file + " HTTP/1.1 uResponse_time - msec " + Math.random()*5000000000 + " request_time 0.000 ");
  var refer_url = "http://webflow-sites-prod.s3.amazonaws.com/" + web + file;
  log += refer_url;
  return log;
}

function mainProc(everything, callback) {
  //executed on each chunk split by "\n" from all Logs
  putInArr(everything, function(err, data) {
    if(err) {
      console.log("STUPID ERR: " + err);
      return callback(err);
    } else { 
      async.each(data, parseAll, function(err) {
        if(err) console.log("err from 198: " + err);
        return callback(err);
      }); 
    }
  });
}

function parseAll(item, callback) {
  //if it contains this that means it's just a ping
  if(item.indexOf("-_-") !== -1 || item === "" || !item || item === "\n")  {
    return callback();
  } 
  var arr = item.split(' ');
  var obj = {};

  async.parallel([
    function(cb) {
      //splits between [day]/[month]/[year] * : * [time]:[time]:[time]   
      var first = arr[0].substring(1, arr[0].indexOf(":")); //the one is because of the first bracket
    
      //then moment it to turn it into moment object
      try {
        var mom_date = moment(first).format("YYYYMMDD");
      } catch(err) {
        return callback();
      }
      if(mom_date === null) {
          return cb(item);
      }
      obj = {
        date: mom_date   //should check if contains 2 colons
      }; 
      cb(null);
    }, function(cb) {
      /* 
        go through all of the ones split by spaces and find one that...
        INCLUDES- "-" and "/" and "."
        DOES NOT INCLUDE- "\n" 
      */
      for(var i =0, cont = true; i < arr.length && cont; i++)
      {
        if(arr[i].indexOf("-") !== -1 && arr[i].indexOf("/") !== -1 && arr[i].indexOf("\n") === -1 && arr[i].indexOf(".") !== -1) {
          //split the one that contains the "-" 
          //this div takes the part between the ip and the domain, which is linked by a hyphen
          var div = arr[i].substring(arr[i].indexOf("-") + 1, arr[i].length);
          //substring up to the "/"
          obj.domain = div.substring(0, div.indexOf("/"));
          //take everything from the "/" and afterwards to get the entire file
          obj.file   = div.substring(div.indexOf("/"), div.length);
          cont = false;
          //console.log(JSON.stringify(obj, null, '\t'));
        } 
      } 
      cb(null);
    }], function(err, res) {
      //if obj.domain doesn't exist in bucketarr then create it, otherwise update
      if(!obj.domain || !obj.file)
      {
        if(item.indexOf(".com") !== -1) {
          console.log("going to fail at this item: " + item);
          return callback(item);
        }
      } 
      else {
        //starts off undefined and then finds the domain of the item
        //if not found, leave undefined
        var result = undefined;
        for(var i = 0, cont = true; i < allWebs.length && cont; i++)
        { 
          if(allWebs[i].domain === obj.domain) {
            result = allWebs[i];
            cont = false;
          }
        }
        //meaning this domain wasn't found in the allWebs object and hasn't been created yet
        if(result === undefined)
        {
          //for each website hit, it must contain these things to be stored into mongodb
          var web = {
            domain: obj.domain,
            count: 1,
            fileAccess: [{
              count: 1,
              file: obj.file,
              dateAccess: [obj.date]
            }],
            dateAccess: obj.date
          };
          allWebs.push(web);
          callback();
          //created then callback
        } else {  
          //otherwise update the count
          result.count++;
          //push in the new dates
          //result.dateAccess.push(obj.date);
        
          //check if file already exists, if not then create it, if does, then count inc, and push date
          var fileresult = undefined;
          for(var i = 0, cont = true; i < result.fileAccess.length && cont; i++)
          {
            if(result.fileAccess[i].file === obj.file)
            {
              fileresult = result.fileAccess[i];
              cont = false;
            }
          }
          //If a file with this name is not found create it and push it into the fileAccess 
          if(fileresult === undefined) {
            //create file 
            var fileAccess = { 
              count: 1,
              file: obj.file,
              dateAccess: [obj.date]
            };
            result.fileAccess.push(fileAccess);
          } else {
            //otherwise just update the counts
            fileresult.count++;
            fileresult.dateAccess.push(obj.date);
          }        
          callback();
        }
      }
    });
  }

/**
  This just gives a randomDate given two jscript date objects
 */
function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

/**
  PRE REQ- EVERYTHING OBJECT MUST BE FILLED
  Then we split up everything into alllogs with the "\n" as the split
  */
function putInArr(everything, cb) {
  if(!everything || everything === "") {
    return cb("not everything");
  }
  allLogs = everything.split('\n');
  //console.log("JSON: " + JSON.stringify(allLogs, null ,'\t'));
  return cb(null, allLogs);
}

/*
  Main Stream handles the data processing by streaming the huge chunks of data
  given by s3 OR written as pseudo logs
 */
var MyStream = function(finalcb) {
  
  //stream settings
  this.writable = true;
  this.readable = true;
  
  //variables to be used
  var source;
  var buffer = '';
  var self = this;
  
  //the chunk of data gets split into 2 bunches
  var first = "";
  var second = ""; //the second part that gets delayed to the next half
  var everything = "";

  this.write = function(data, encoding) {
    var strung = data.toString(encoding || 'utf8');
    var last = strung.lastIndexOf("\n");
    first = strung.substring(0, last);
    everything = second + first; // add everything from the previous chunk and the current chunk
    
    second = strung.substring(last, strung.length);
    //must set the everything object
    //call the main process to execute on every chunk of data written
    mainProc(everything, function(err) { 
      if(err ) { 
        console.log("err: " + err);
        return; 
      }
    });
  };

  //on function end we still have data in the second half? we shouldn't
  this.end = function() {
    if(second !== "" && second !== "\n") {
      console.log("err second not empty or new line: " + second);
    }
    //benchmarks the start of sorting 
    //get the ranks by sorting by count
    async.sortBy(allWebs, function(item, cb) {
      cb(null, item.count);
    }, function(err, results) {
      //store everything into the final object
      finalObject = {
        ranks: results,
        info: allWebs
      };
      
      //emit the finalobject and initiate callback [don't actually need this though]
      self.emit('data', JSON.stringify(finalObject, null, '\t'));
      finalcb(err, finalObject);
    });
  };

  this.on('pipe', function(src) {
    source = src;
  });
  
  this.pause = function() {
    if (!source) {
      throw new Error('pause() only supported when a LineStream is piped into');
    }
    //source.pause();
  };
  
  this.resume = function() {
    if (!source) {
      throw new Error('resume() only supported when a LineStream is piped into');
    }
    //source.resume();
  };
};

//inheritance of MyStream with the Nodejs stream
require('util').inherits(MyStream, require('stream').Stream);

/**
This one has been taken out because it's too slow and unneeded for now
But it can be used to sort the dates if one would want to
This should be changed to use momentjs to be faster
**/
function dateSort(dateArr, cb ) {
  var  map = [], result = [];
  for( var i in dateArr) {
     map.push({
        index: i, //remember index
        valueArr: dateArr[i].split(":"), //split into array
        orig: dateArr[i]
     });
  }
  map.sort(function(a, b) {
    var ymdA = a.valueArr[0].split("/"); //yearmonth day
    var ymdB = b.valueArr[0].split("/"); //yearmonth day
    var dateA = new Date(ymdA[1] +" " + ymdA[2]+ ", "+ ymdA[0]+ " " + a.orig.substring(a.orig.indexOf(":"), a.orig.length));
    var dateB = new Date(ymdB[1] +" " + ymdB[2]+ ", "+ ymdB[0]+ " " + b.orig.substring(b.orig.indexOf(":"), b.orig.length));
    return dateA > dateB ? 1: -1;
  });
  for(var i in map) {
    result.push(dateArr[map[i].index]);
  }
  cb(null, result);
}

/**
  MODULE EXPORTS:
    [no name]- request url pipe through stream, and go through destination
  return_webs- just returns the allWebs, but it must be set first
  exists- checks if the finalObject jscript object has been set by just returning it
  main- used for testing when combined with the write/:num to data.txt, pipes through stream, and gives the callback of data
  doeverything- function 
  **/
module.exports = function(s3url, domain, query, destination) {
  request(s3url)
     .pipe(new MyStream()).pipe(destination);
   //btw doesn't require the callback because there's a self.emit(data)
}

module.exports.return_webs = function(cb) {
  cb(allWebs);
}
module.exports.exists = function(cb) {
  cb(null, finalObject);
}

module.exports.main = function(callback) {
  console.log("executing main....");

   var result = fs.createReadStream('data.txt')
                  .pipe(new MyStream(function(err, data) {
                    callback(err, data);
                  }));
}

//requires a date start and date end, and a hostname
//(isn't being used right now), but can be to query multiple days, though it takes a while
module.exports.doeverything = function(start, end, hostnames, s3, callback) {
  allWebs = [];
  var dates = [ 
    "20130802"// begin 0802
  ];
  
  //get the start,end 
  var start = moment(start); //was 2013, 7, 2 
  var end   = moment(end);
  
  //get the difference and push that stuff into dates
  var diff = start.diff(end, 'days');
  //console.log("diff: " + diff);
  for(var i = 0; i < diff - 1; i++) {
     dates.push(start.add('d', 1).format("YYYYMMDD"));
  } 
  
  /*
   objective query each date with async and use the knox and zlib to help out and pipe into MyStream
   series is because we have 2 buckets, proxy02a and proxy01d
  */
  async.series([
    function(series_cb) { 
      //the following series do the same thing, but changes the hostname
      //but it creates the url for s3 to get, callback gets the res which pipes into a zlib unzipper, and finally into mystream
      async.each(dates, function(item, cb) {
        var url = '/' + 'proxy02a' + '/sites.access.log-' + item + '.gz';
        s3.get(url).on('response', function(res) {
          res.pipe(zlib.createGunzip()).pipe(new MyStream(function(err, data) {
            cb(err);  
          }));
        }).end();
      }, function(err) { 
        series_cb(err);
      });
    }, 
    function(series_cb) { 
      async.each(dates, function(item, cb) {
      var url = '/' + 'proxy01d' + '/sites.access.log-' + item + '.gz';
      s3.get(url).on('response', function(res) {
        res.pipe(zlib.createGunzip()).pipe(new MyStream(function(err, data) {
          cb(err);  
        }));
      }).end();
    }, function(err) { 
      series_cb(err);
    });
  }], function(err,results){callback(err, allWebs, dates)});
}

/**
  Main function to query the date given to the function 
 */
module.exports.queryday = function(date, s3, hostnames, callback) {
  //reset allWebs, and initialize the date to be pushedi nto 
  allWebs= [];
  var dates = date.format("YYYYMMDD");

  //as soon as first bucket finishes, do the next bucket
  async.eachSeries(hostnames, function(item, cb) {
    var url = '/' +  item + '/sites.access.log-' + dates + '.gz';
    s3.get(url).on('response', function(res) {
      if(res.statusCode !== 200) return cb("error, status is " + res.statusCode + " for this s3 url " + url);
      res.pipe(zlib.createGunzip()).pipe(new MyStream(function(err, data) {
        cb(err);  
      }));
    }).end();
  }, function(err){
    callback(err, allWebs, dates)
  });
}

//this one just queries the current day, which might not be super useful because not all the data is written yet
//but there's no need to unzip from zlib 
//requires the hostnames as an array type object
module.exports.current = function(hostnames, s3, callback) {
  allWebs = [];
  async.each(hostnames, function(item, cb) {
    s3.get('/' + item + '/sites.access.log').on('response', function(res) {
      if(!res || res === undefined) return cb("no res");
      res.pipe(new MyStream(function(err, data) {
        cb(err);
      }));
    }).end();
  }, function(err) {
     callback(err, allWebs);
  });
}

/*
Mocha testing: (if needed)
1) request('http://0.0.0.0:8081/write/500000'); //write 500,000 random access logs
  console.log("writing random access logs");
2) fs.createReadStream('data.txt') //instead of writing 500,000 just pipe it into a stream
     .pipe(new MyStream(urls[random] +".com", "select file")).pipe(check_stream); //pipe the result into a checking stream
3) Use should
4) return results   
*/
