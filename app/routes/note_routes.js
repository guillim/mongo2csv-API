const utils = require('../../lib/utils.js');

module.exports = function(app, db) {
  app.get('/status', (req, res) => {
    res.send('connected to the database')
  });

  // a route to create a CSV by fetching data from MongoDB
  app.get('/getcsv/:collection/:id/:param', (req, res) => {
    if(!req.params.collection) throw 'collection required'
    if(!req.params.id) throw 'id required'
    if(!req.params.param) throw 'param required'

    let debut = new Date().getTime()
    console.log("mongo2csv-API - id:",req.params.id, ' param=',req.params.param, ' collection=',req.params.collection)

    db.collection(req.params.collection)
    .find({
      postid:req.params.id,
      "crawlerFinishedAt": { $gt: new Date(new Date().setDate(new Date().getDate()-req.params.param))}
    },
    {
      fields:{
        "postid":0,
        "rowCreatedAt":0,
        "debugInfo":0
      }
    })
    .limit(500000)
    .sort({rowCreatedAt:-1})
    .toArray(function(err,result) {
      if (err) throw err;
      if(result){
				if (result.length > 0) {
								let obj = utils.fileTreatment(result,[{columnName:"crawlerFinishedAt",format:"DD/MM/YYYY HH:mm"}])
                console.log('fileTreatment finished at ', (new Date().getTime() - debut) / 1000  );
								if(obj.bool){		result =	utils.fileParse(obj.collection)
								}else{						console.log('inner inner else');
								}
				}else{  console.log('result is empty');
				}
	    }else{ console.log('result is not defined');
			}
      console.log((new Date().getTime() - debut) / 1000  );
      res.send(result)
    });
  });
};
