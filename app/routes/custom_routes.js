const utils = require('../../lib/utils.js');
const utilsStream = require('../../lib/utilsStream.js');
const fs = require('fs-extra');
const JSONStream = require('JSONStream')

module.exports = function(app, db) {
  app.get('/getcsvwetransferemailNoStream_customRoute/:collection/:id/:param1/:param2/:email', async (req, res) => {
    if(!req.params.collection) throw 'collection required'
    if(!req.params.id) throw 'id required'
    if(!req.params.param1) throw 'param1 oldest required'
    if(!req.params.param2) throw 'param2 recent required'
    if(!req.params.email) throw 'email required'

    let debut = new Date().getTime()
    console.log("mongo2csv-API - id:",req.params.id, ' param1=',req.params.param1,' param2=',req.params.param2, ' collection=',req.params.collection, ' email=',req.params.email)



    //here we define two funcitons to get some data
    const getPost = async function(){ return await db.collection('posts').findOne({_id:req.params.id}) }
    //needs to be here otherwise notenough time for keywords to use post.owner
    let post = await getPost();


    const getKeywords = async function(){ return await  db.collection('keywords').find({postid:req.params.id}).toArray() }
    const getUtils = async function(p){  return await db.collection('utils').findOne({ $or: [{otherUsers:p.owner},{owner:p.owner}]})   }
    // const getPositionToCTRs = async function(p){  return await db.collection('utils').findOne({ $or: [{otherUsers:p.owner},{owner:p.owner}]}).positionToCTRs    }


    // HERE WE BEGIN

    //we fetch all needed data for all results
    let keywords = await getKeywords();
    let utils_positionToCTRs = await getUtils(post);

    if(!post) {res.send({status : 'aborted', msg:'post undefined'})}
    console.log('post.owner:',post.owner);
    // console.log('getKeywords:',keywords);
    // console.log('utils_positionToCTRs:',utils_positionToCTRs);
    let message = 'Not set up'



    db.collection(req.params.collection)
    .find({
      postid:req.params.id,
      "crawlerFinishedAt": { $gte: new Date(new Date().setDate(new Date().getDate()-req.params.param1)), $lte: new Date(new Date().setDate(new Date().getDate()-req.params.param2)) }
    },
    {
      fields:{
        // "postid":0,
        "rowCreatedAt":0,
        "debugInfo":0
      }
    })
    .limit(500000)
    // .limit(5)
    .sort({rowCreatedAt:-1})
    .toArray( async function(err,results) {
      if (err) throw err;
      if(results && results.length > 0) {


          const processEachRow = function(obj,post,positionToCTRs,keyword){
              if (post.crawlerCategory === 'simple' || post.crawlerCategory === 'profond') {
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
                let positionToCTR = positionToCTRs.find( (o) => { return o.position === obj.c07_position;  });
                obj.ctr = (positionToCTR && positionToCTR.CTR) ? positionToCTR.CTR : message
                obj.score = (obj.ctr !== message && obj.c01_keyword_searchVolume !== message) ? parseFloat(obj.ctr) * parseFloat(obj.c01_keyword_searchVolume) : message
                return obj
              } else { return obj  }
          }


          //now we go through each row and we add the Required column (from what client says)
          let fullResult = []

          results.map( (result) => {
            let keyword = keywords.find( (el) => { return el.keyword === result.c01_keyword;  });
            if (!keyword) {   fullResult.push( result )}
            else{             fullResult.push( processEachRow(result, post, utils_positionToCTRs.positionToCTRs, keyword) )}
          })

          //now that we have all the results in one variable again, we apply the classic treatment
          let fullResultFieldsDeleted = utils.deleteFields(fullResult,['postid','_id'])
          let fullResultobj = utils.fileTreatment(fullResultFieldsDeleted,[{columnName:"crawlerFinishedAt",format:"DD-MM-YYYY"}])
          console.log('fileTreatment finished at ', (new Date().getTime() - debut) / 1000  );

          if(fullResultobj.bool){
            let resultFortransfer =	utils.fileParse(fullResultobj.collection)
            //now we launch the wetransfer funcitonality
            utils.wetransferProcess(resultFortransfer,'dontgomanual_'+req.params.id+'.csv',req.params.email,res)
          }else{
            console.log('inner inner else');
            utils.wetransferProcess('problem in the treatment','dontgomanual_'+req.params.id+'.csv',req.params.email,res)
          }

      }else{  console.log('result is empty or undefined');
      }
      console.log((new Date().getTime() - debut) / 1000  );
    });
  }),



  app.get('/getcsvwetransferemail_customRoute/:collection/:id/:param1/:param2/:email', async (req, res) => {
      if(!req.params.collection) throw 'collection required'
      if(!req.params.id) throw 'id required'
      if(!req.params.param1) throw 'param1 oldest required'
      if(!req.params.param2) throw 'param2 recent required'
      if(!req.params.email) throw 'email required'

      let debut = new Date().getTime()
      console.log("mongo2csv-API - id:",req.params.id, ' param1=',req.params.param1,' param2=',req.params.param2, ' collection=',req.params.collection, ' email=',req.params.email)



      //here we define two funcitons to get some data
      const getPost = async function(){ return await db.collection('posts').findOne({_id:req.params.id}) }
      //needs to be here otherwise notenough time for keywords to use post.owner
      let post = await getPost();


      const getKeywords = async function(){ return await  db.collection('keywords').find({postid:req.params.id}).toArray() }
      const getUtils = async function(p){  return await db.collection('utils').findOne({ $or: [{otherUsers:p.owner},{owner:p.owner}]})   }
      // const getPositionToCTRs = async function(p){  return await db.collection('utils').findOne({ $or: [{otherUsers:p.owner},{owner:p.owner}]}).positionToCTRs    }


      // HERE WE BEGIN

      //we fetch all needed data for all results
      let keywords = await getKeywords();
      let utils_positionToCTRs = await getUtils(post);

      if(!post) {res.send({status : 'aborted', msg:'post undefined'})}
      console.log('post.owner:',post.owner);
      // console.log('getKeywords:',keywords);
      // console.log('utils_positionToCTRs:',utils_positionToCTRs);
      let message = 'Not set up'


      const jsoniniFullPath = './app/tmp/' + new Date().getTime() + '.jsonini'
      const output = fs.createWriteStream(jsoniniFullPath, { encoding: 'utf8' });

      let processor = db.collection(req.params.collection)
      .find({
        postid:req.params.id,
        "crawlerFinishedAt": { $gte: new Date(new Date().setDate(new Date().getDate()-req.params.param1)), $lte: new Date(new Date().setDate(new Date().getDate()-req.params.param2)) }
      },
      {
        fields:{
          // "postid":0,
          "rowCreatedAt":0,
          "debugInfo":0
        }
      })
      .limit(500000)
      // .limit(5)
      .sort({rowCreatedAt:-1})
      .stream()
      .pipe(JSONStream.stringify() )
      .pipe(output)

      processor.on('finish', () => {   utilsStream.fullProcess(jsoniniFullPath,post,keywords,utils_positionToCTRs,message,res,req,db)  });
      processor.on('error', (err) => console.log('error in jsoniniFullPath',err) );

      res.send('result processing, you will soon receive an email at ' + req.params.email)
  })

};
