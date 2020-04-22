const utilsStream = require('../../lib/utilsStream.js');
const JSONStream = require('JSONStream')
const QueryStream = require('pg-query-stream')
const fs = require('fs-extra');

module.exports = function(app, client, done) {
  app.get('/getcsvwetransferemailpostgresStreaming_customRoute/:table/:id/:param1/:param2/:email', async (req, res) => {
      if(!req.params.table) throw 'table required'
      if(!req.params.id) throw 'id required'
      if(!req.params.param1) throw 'param1 oldest required'
      if(!req.params.param2) throw 'param2 recent required'
      if(!req.params.email) throw 'email required'

      let debut = new Date().getTime()
      console.log("mongo2csv-API - id:",req.params.id, ' param1=',req.params.param1,' param2=',req.params.param2, ' table=',req.params.table, ' email=',req.params.email)

      const colums = "keyword,marketplace,numberofresults,asin,sponsoredbrand,sponsoredproduct,itemurl,position,title,numberofcomments,ispantry,isprime,isnumberoneseller,isfirstchoice,iscouponavailable,url,crawlertime,brand,campagne,category,subcategory,custom,searchvolume,cornerbrand,crawlertitle,crawler_id,ctr,score,seller,sellerofficial,star,sellertechnicalbrand,iscornerbrandproduct";

      const query = new QueryStream("SELECT "+colums+" FROM " +req.params.table+" WHERE crawler_id = $1 AND CAST(crawlertime AS date) BETWEEN CAST((CAST(now() AS timestamp) + (INTERVAL '-" + req.params.param1 +" day'" + ")) AS date) AND CAST((CAST(now() AS timestamp) + (INTERVAL '-" + req.params.param2 +" day'" + ")) AS date)", [req.params.id])

      const jsoniniFullPath = './app/tmp/' + new Date().getTime() + '.jsonini'
      const output = fs.createWriteStream(jsoniniFullPath, { encoding: 'utf8' });

      const stream = client.query(query)

      stream.pipe(JSONStream.stringify()).pipe(output)

      stream.on('end', () => { utilsStream.fullProcessPostgres(jsoniniFullPath,req) });
      stream.on('error', (err) => console.log('error in jsoniniFullPath',err) );
      res.send('result processing, you will soon receive an email at ' + req.params.email)
  });
};
