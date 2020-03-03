const utilsStream = require('../../lib/utilsStream.js');
const fs = require('fs-extra');
const JSONStream = require('JSONStream')

module.exports = function(app, db) {
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
