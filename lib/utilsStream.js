const Json2csvTransform = require('json2csv').Transform
const moment = require('moment');
const map = require('through2-map');
const filter = require('through2-filter');
const JSONStream = require('JSONStream')
const fs = require('fs-extra');
const createWTClient = require('@wetransfer/js-sdk');
const api_keys       = require('../config/api_keys');
const email_utils = require('./email_utils.js');
const utils = require('./utils.js');


const deleteFieldsStream = function(obj,fieldsArray) {
  // console.log('Entering - deleteFieldsStream');
	try {
		fieldsArray.forEach((field) => {			if(obj.hasOwnProperty(field)){delete obj[field];}		})
		return obj
	} catch (err) {    return {err:'err while deleteFieldsStream - ' + err.toString().slice(0,30) + '...' }  }
};

const fileFormatStream = function(obj,dateFormatObjects) {
  // console.log('Entering - fileFormatStream');
	try {
			dateFormatObjects.forEach((dateFormat) => {
				obj[dateFormat.columnName] =  (obj[dateFormat.columnName]) ? moment(obj[dateFormat.columnName]).format(dateFormat.format) : ''
			})
		 let flatObj = utils.flatten(obj)
     return flatObj
	} catch (err) {    return {err:'err while fileFormatStream - ' + err.toString().slice(0,30) + '...' }  }
};

//pour filter un obj vide, on arrete ici
const filterResultsStream = function(obj) {
  // console.log('Entering - filterResultsStream');
  return !!obj.c02_marketplaceName && obj.c02_marketplaceName !== ''
}


const processResultsStream = function(obj,options) {
  let keywords = options.keywords
  let sellerASIN = options.sellerASIN
  let post = options.post
  let positionToCTRs = options.positionToCTRs
  let message = options.message

	try {
  		let keyword = keywords.find( (el) => { return el.keyword === obj.c01_keyword;  });
  		if ( (!obj.c09_seller || obj.c09_seller === '') && (typeof obj.c04_asin === 'string' && obj.c04_asin !== '') && sellerASIN && sellerASIN.get(obj.c04_asin) ){
  			obj.c09_seller = sellerASIN.get(obj.c04_asin)
  		}
  		if (!keyword) {  return obj }
  		else{            return  processEachRow(obj, post, positionToCTRs, keyword, message) }
	} catch (err) {    return {err:'err while processResultsStream - ' + err.toString().slice(0,30) + '...' }  }
};

const fileTreatmentStream = async function(optionsObj,inputPath,outputPath){
  // console.log('Entering - fileTreatmentStream');
  let debut = new Date().getTime()
  let deleteFields = optionsObj.deleteFields
  let dateFormatObjects = optionsObj.dateFormatObjects
  let optionsProcessResultsStream = optionsObj.optionsProcessResultsStream


  try {
		const input = fs.createReadStream(inputPath, { encoding: 'utf8' });
		const output = fs.createWriteStream(outputPath, { encoding: 'utf8' });
		const processor = input
    .pipe(JSONStream.parse('.*'))
    .pipe(filter({objectMode: true}, filterResultsStream))
    .pipe(map({objectMode: true}, function (obj) {
      let objTreated = processResultsStream(obj,optionsProcessResultsStream)
      return objTreated
    }))
    .pipe(map({objectMode: true}, function (obj) {
      let objCleant = deleteFieldsStream(obj,deleteFields)
      let objTreated = fileFormatStream(objCleant,dateFormatObjects)
      return objTreated
    }))
    .pipe(map({objectMode: true}, function (obj) {
      // we delete the extra line breaks that can be annoying for excel
      let objClean = JSON.stringify(obj)
      let objClean2 = objClean.replace(/\r?\n|\r/g," ")
      let objClean3 = JSON.parse(objClean2)
      return objClean3
    }))
    .pipe(JSONStream.stringify() )
    .pipe(output)

		let end = new Promise(function(resolve, reject) {
	    processor.on('finish', () => resolve(true) );
	    processor.on('error', () => reject(false) );
		});

    console.log('Time - fileTreatmentStream:', (new Date().getTime() - debut) / 1000 );
		return await end

  } catch (err) {    console.error('err in fileTreatmentStream',err); return false
  }
}


let fileParseStream = async function(headers,inputPath,outputPath){
  let opts;
  let debut = new Date().getTime()

	try {
    opts = {
      fields:headers,
      delimiter: ',',
      eol: '\r\n'
    };
    // console.log(opts);
  } catch (err) { console.log('err in buildCSVHeader',err); return false
  }
  // console.log('Entering - Json2csvTransform');

  try {
		const transformOpts = { highWaterMark: 16384, encoding: 'utf-8' };
		const input = fs.createReadStream(inputPath, { encoding: 'utf8' });
		const output = fs.createWriteStream(outputPath, { encoding: 'utf8' });
		const json2csv = new Json2csvTransform(opts,transformOpts);
		const processor = input.pipe(json2csv).pipe(output)

		let end = new Promise(function(resolve, reject) {
	    processor.on('finish', () => resolve(true) );
	    processor.on('error', () => reject(false) );
			json2csv.on('error', () => reject(false) );
		});

		return await end

  } catch (err) {    console.error('err in Json2csvTransform',err); return false
  }
}



const wetransferProcess = async function(finalResults,name,email){
    const wtClient = await createWTClient(api_keys.wetransfer);
    const content = await Buffer.from(finalResults);

    if(content.length <= 1) {console.log('ERROR: content.length <= 1')}
    const transfer = await wtClient.transfer.create({
      message: 'From dontgomanual: Thanks you for using our service!',
      files: [
        {
          name: name,
          size: content.length,
          content: content
        }
      ]
    });

    let emailProcessStatus = email_utils.sendEmail({
      to:decodeURI(email),
      link:transfer.url
    })

    let responseObj = {}
    responseObj.status = emailProcessStatus
    responseObj.content = transfer.url
}

const deleteOldFiles = function(){
	// console.log('Entering - deleteOldFiles');
	let old = new Date();
  old.setMinutes(old.getMinutes() - 3);
	let oldNumber = old.getTime()

	fs.readdir('./tmp/', function (err, files) {
	    if (err) { return console.log('Unable to scan directory: ' + err); }
	    files.forEach(function (file) {
				let date = parseInt(file.replace(/\D/,''))
				// si le fichier a ete creee il ya plus d'1 heure, alors on supprime
				if( date < oldNumber) {
					fs.unlink('./tmp/'+file, (err) => {
					  if (err) throw err;
					  console.log(file,' was deleted');
					});
				}
	    });
	});

}


const processEachRow = function(obj,post,positionToCTRs,keyword,message = 'Not set up'){
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


const getSeller = async function(asin,db,req){
  return db.collection(req.params.collection)
  .find( { "c04_asin": asin, $and: [ { "c09_seller": { $exists: true } }, { "c09_seller": { $ne: "" } } ] }, {fields: {"c09_seller":1,"c04_asin":1}} )
  .sort({rowCreatedAt:-1})
  .limit(1)
  .toArray()
}

const getSellerASINlink = async function(results,req,db){
  try {
		//on construit l'ensemble des valeurs unique de ASIN:
		let arr = results.map(o => o['c04_asin']);
		let setOfASINs = new Set(arr);
		let asinMap = new Map();

		let promiseArray = []
		for (const asin of setOfASINs) { promiseArray.push(getSeller(asin,db,req)) }

		return Promise.all(promiseArray).then(
			values => {
				// console.log('Promise.all val:',values);
				values.forEach( (val) => {
					if(val && val[0] && val[0]['c04_asin'] && val[0]['c09_seller']){ asinMap.set(val[0]['c04_asin'], val[0]['c09_seller']) }
				})
				return asinMap
			},
			err => {
				console.log('Promise.all err',err);
				return false
			}
		)
  } catch (err) {    console.error('err in getSellerASINlink',err); return false
  }
}


const fullProcess = async function(results,post,keywords,utils_positionToCTRs,message,res,req,db){
	let debut = new Date().getTime()
	//now we go through each row and we add the Required column (from what client says)
	let fullResult = []

  //let's delete old big files (older than 3 minutes).
  deleteOldFiles()
  let fieldsToBeDeleted = ['postid','_id','errorInfo']
  let dateFormatObjects = [{columnName:"crawlerFinishedAt",format:"DD-MM-YYYY"}]

	let sellerASIN = await getSellerASINlink(results,req,db)
	console.log('Note: sellerASIN at ', (new Date().getTime() - debut) / 1000  );

  let headers = utils.buildCSVHeader(results);
  headers = headers.filter(header => fieldsToBeDeleted.indexOf(header) === -1 );

  let jsoniniFullPath = './tmp/' + new Date().getTime() + '.jsonini'
  let jsonFullPath = jsoniniFullPath.replace(/jsonini/,'json')

  utils.writeAFile(results,jsoniniFullPath)

  //relesasing the memory
  results = null

  let optionsProcessResultsStream = {
    keywords : keywords,
    sellerASIN : sellerASIN,
    post : post,
    positionToCTRs : utils_positionToCTRs.positionToCTRs,
    message : message
  }

  let treatmentOutput = await fileTreatmentStream({
      deleteFields:fieldsToBeDeleted,
      dateFormatObjects: dateFormatObjects,
      optionsProcessResultsStream: optionsProcessResultsStream
    },jsoniniFullPath,jsonFullPath)

  console.log('Time - fileTreatment:', (new Date().getTime() - debut) / 1000  );

	if(treatmentOutput){
		let csvFullPath = jsonFullPath.replace(/.json$/,'.csv')

		let boo = await fileParseStream(headers,jsonFullPath,csvFullPath)
    console.log('Time - fileParseStream:', (new Date().getTime() - debut) / 1000  );

		console.log('Note: resultat de fileParseStream=',boo);
		if(boo) {
			let fileCSV = fs.readFileSync(csvFullPath);
			wetransferProcess(fileCSV,'dontgomanual_'+req.params.id+'.csv',req.params.email)
		} else {
			wetransferProcess('problem in the treatment','dontgomanual_'+req.params.id+'.csv',req.params.email)
		}
	}else{
		wetransferProcess('problem in the treatment','dontgomanual_'+req.params.id+'.csv',req.params.email)
	}

}

module.exports.fullProcess = fullProcess
