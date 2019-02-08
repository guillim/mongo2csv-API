const utils = require('../../lib/utils.js');
const createWTClient = require('@wetransfer/js-sdk');
const api_keys       = require('../../config/api_keys');

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

      let responseObj = {}
      responseObj.status = true
      responseObj.content = result

      res.send(responseObj)
    });
  });




  app.get('/getcsvwetransfer/:collection/:id/:param', (req, res) => {
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

      (async function() {
        // An authorization call is made when you create the client.
        // Keep that in mind to perform this operation
        // in the most suitable part of your code
        const wtClient = await createWTClient(api_keys.wetransfer);

        // const content = Buffer.from('Look ma, a transfer!');
        const content = Buffer.from(result);
        const transfer = await wtClient.transfer.create({
          message: 'From dontgomanual: Thanks you for using our service!',
          files: [
            {
              name: 'dontgomanual_'+req.params.id+'.csv',
              size: content.length,
              content: content
            }
          ]
        });

        console.log(transfer.url)

        let responseObj = {}
        responseObj.status = true
        responseObj.content = transfer.url

        res.send(responseObj)
      })();


    });
  });

};
