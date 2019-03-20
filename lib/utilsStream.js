const Json2csvTransform = require('json2csv').Transform
const fs = require('fs-extra');
const createWTClient = require('@wetransfer/js-sdk');
const api_keys       = require('../config/api_keys');
const email_utils = require('./email_utils.js');
const utils = require('./utils.js');

let fileParseStream = async function(collection,inputPath){
  let opts;
  let debut = new Date().getTime()

	try {
    const headers = utils.buildCSVHeader(collection);
    opts = {
      fields:headers,
      delimiter: ',',
      eol: '\r\n'
    };
    // console.log(opts);
  } catch (err) { console.log('err in buildCSVHeader',err); return false
  }
  console.log('time for buildCSVHeader');
  console.log((new Date().getTime() - debut) / 1000  );

  try {
		let outputPath = inputPath.replace(/.json$/,'.csv')

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

  } catch (err) {    console.error('err in Json2csvParser',err); return false
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
	console.log('deleteOldFiles');
	let old = new Date();
	old.setHours(old.getHours()-1);
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



const fullProcess = async function(results,post,keywords,utils_positionToCTRs,message,res,req){
	let debut = new Date().getTime()
	//now we go through each row and we add the Required column (from what client says)
	let fullResult = []

	results.map( (result) => {
		let keyword = keywords.find( (el) => { return el.keyword === result.c01_keyword;  });
		if (!keyword) {   fullResult.push( result )}
		else{             fullResult.push( processEachRow(result, post, utils_positionToCTRs.positionToCTRs, keyword, message) )}
	})

	//now that we have all the results in one variable again, we apply the classic treatment
	let fullResultFieldsDeleted = utils.deleteFields(fullResult,['postid','_id'])
	let fullResultobj = utils.fileTreatment(fullResultFieldsDeleted,[{columnName:"crawlerFinishedAt",format:"DD-MM-YYYY"}])
	console.log('map + deleteFields + fileTreatment finished at ', (new Date().getTime() - debut) / 1000  );

	deleteOldFiles()

	if(fullResultobj.bool){

		let file = fullResultobj.collection
		let fileInputFullPath = './tmp/' + new Date().getTime() + '.json'
		let fileOutputFullPath = fileInputFullPath.replace(/.json$/,'.csv')

		utils.writeAFile(file,fileInputFullPath)
		let boo = await fileParseStream(file,fileInputFullPath)

		console.log('resultat de fileParseStream: ',boo);
		if(boo) {
			let fileCSV = fs.readFileSync(fileOutputFullPath);
			wetransferProcess(fileCSV,'dontgomanual_'+req.params.id+'.csv',req.params.email)
		} else {
			wetransferProcess('problem in the treatment','dontgomanual_'+req.params.id+'.csv',req.params.email)
		}
	}else{
		wetransferProcess('problem in the treatment','dontgomanual_'+req.params.id+'.csv',req.params.email)
	}

}

module.exports.fullProcess = fullProcess
