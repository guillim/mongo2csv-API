const utilsStream = require('../../lib/utilsStream.js');
const Json2csvParser = require("json2csv").Parser;

module.exports = function(app, client, done) {
  app.get('/getcsvwetransferemailpostgres_customRoute/:table/:id/:param1/:param2/:email', async (req, res) => {
      if(!req.params.table) throw 'table required'
      if(!req.params.id) throw 'id required'
      if(!req.params.param1) throw 'param1 oldest required'
      if(!req.params.param2) throw 'param2 recent required'
      if(!req.params.email) throw 'email required'

      let debut = new Date().getTime()
      console.log("mongo2csv-API - id:",req.params.id, ' param1=',req.params.param1,' param2=',req.params.param2, ' table=',req.params.table, ' email=',req.params.email)

      const query = {
        text: "SELECT * FROM " +req.params.table+" WHERE crawler_id = $1 AND CAST(crawlertime AS date) BETWEEN CAST((CAST(now() AS timestamp) + (INTERVAL '-" + req.params.param1 +" day'" + ")) AS date) AND CAST((CAST(now() AS timestamp) + (INTERVAL '-" + req.params.param2 +" day'" + ")) AS date)",
        values: [req.params.id],
      }
      client.query(query, (err, results) => {
        if (err) {  console.log(err.stack);  res.send('problem') }
        else {
          const jsonData = JSON.parse(JSON.stringify(results.rows));
          if (jsonData.length === 0) {  res.send('problem - empty json') }
          const json2csvParser = new Json2csvParser({ header: true });
          //to avoid waiting too long
          res.send('done')
          const csv = json2csvParser.parse(jsonData);
          utilsStream.wetransferProcess(csv,'dontgomanual_'+req.params.id+'.csv',req.params.email)
        }
      });
  });
};
