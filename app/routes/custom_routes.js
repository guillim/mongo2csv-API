const utils = require('../../lib/utils.js');
const email_utils = require('../../lib/email_utils.js');
const createWTClient = require('@wetransfer/js-sdk');
const api_keys       = require('../../config/api_keys');

module.exports = function(app, db) {
  app.get('/getcsvwetransferemail_customRoute/:collection/:id/:param/:email', (req, res) => {
    if(!req.params.collection) throw 'collection required'
    if(!req.params.id) throw 'id required'
    if(!req.params.param) throw 'param required'
    if(!req.params.email) throw 'email required'

    let debut = new Date().getTime()
    console.log("mongo2csv-API - id:",req.params.id, ' param=',req.params.param, ' collection=',req.params.collection, ' email=',req.params.email)

    db.collection(req.params.collection)
    .find({
      postid:req.params.id,
      "crawlerFinishedAt": { $gt: new Date(new Date().setDate(new Date().getDate()-req.params.param))}
    },
    {
      fields:{
        // "postid":0,
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
                console.log('result.length > 0 ');
                let message = 'Not set up'
                try {
                  //we use this funciton in order to be straight away in async mode
                  const startProcess = async function(){
                    await asyncProcessing(result)
                  }

                  //we really begin the process here
                  const asyncProcessing = async function(result){

                    //we fetch all needed data for all results
                    let post = await getPost();
                    let positionToCTRs = await getPositionToCTRs(post);

                    //now we go through each row and we add the Required column (from what client says)
                    await Promise.all(result.map(async (o) => {
                      return await processEachRow(o,post,positionToCTRs)
                    }))
                    .then(function(fullResult) {
                      //now that we have all the results in one variable again, we apply the classic treatment
                      let fullResultFieldsDeleted = utils.deleteFields(fullResult,['postid','_id'])
                      let obj = utils.fileTreatment(fullResultFieldsDeleted,[{columnName:"crawlerFinishedAt",format:"DD-MM-YYYY"}])
                      console.log('fileTreatment finished at ', (new Date().getTime() - debut) / 1000  );
                      if(obj.bool){		fullResult =	utils.fileParse(obj.collection)
                      }else{						console.log('inner inner else');
                      }
                      //now we launch the wetransfer funcitonality
                      wetransferProcess(fullResult)
                    });
                  }


                  //here we define auilary funcitons
                  const getPost = async function(){ return await db.collection('posts').findOne({_id:req.params.id}) }
                  const getPositionToCTRs = async function(post){ return await db.collection('utils').findOne({ $or: [{otherUsers:post.userId},{owner:post.userId}]}).positionToCTRs }
                  const processEachRow = async function(obj,post,positionToCTRs){
                    if (post.crawlerCategory === 'simple' || post.crawlerCategory === 'profond') {

                      const getKeyword = async function(post){ return await db.collection('keywords').findOne({postid:post._id, $and: [ {keyword: obj.c01_keyword } ] }) }
                      let keyword = await getKeyword(post)
                      obj.c01_keyword_categorie = (keyword.category) ? keyword.category : message
                      obj.c01_keyword_sousCategorie = (keyword.subCategory) ? keyword.subCategory : message
                      obj.c01_keyword_custom = (keyword.custom) ? keyword.custom : message
                      obj.c01_keyword_campagne = (keyword.campagne) ? keyword.campagne : message
                      obj.c01_keyword_searchVolume = (keyword.searchVolume) ? keyword.searchVolume : message
                      if (post.listCodeProduit) {
                          obj.c09_seller_official = (post.listCodeProduit.indexOf(obj.c04_asin) === -1) ? keyword.c09_seller : keyword.c09_seller + '_Official'
                      } else {
                          obj.c09_seller_official = message
                      }
                      obj.ctr = (positionToCTRs && positionToCTRs[obj.c07_position]) ? positionToCTRs[obj.c07_position] : message
                      if (obj.ctr !== message) {
                          obj.score = parseFloat(obj.ctr) * parseFloat(obj.c01_keyword_searchVolume)
                      } else {
                          obj.score = message
                      }
                      return await obj
                    } else { return await obj  }
                  }
                  const wetransferProcess = async function(finalResults){
                    const wtClient = await createWTClient(api_keys.wetransfer);
                    const content = await Buffer.from(finalResults);

                    if(content.length <= 1) {res.send('ERROR: content.length <= 1')}
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
                    let emailProcessStatus = email_utils.sendEmail({
                      to:decodeURI(req.params.email),
                      link:transfer.url
                    })

                    let responseObj = {}
                    responseObj.status = emailProcessStatus
                    responseObj.content = transfer.url

                    res.send(responseObj)
                  }
                  startProcess()
                }catch(e){          console.log('try catch failure in the process starting with startProcess()',e)}
        }else{  console.log('result is empty');
        }
      }else{ console.log('result is not defined');
      }
      console.log((new Date().getTime() - debut) / 1000  );
    });
  });
};
