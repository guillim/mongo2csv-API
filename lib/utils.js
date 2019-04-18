const Json2csvParser = require('json2csv').Parser;
const Json2csvTransform = require('json2csv').Transform
const moment = require('moment');
const fs = require('fs-extra');
const createWTClient = require('@wetransfer/js-sdk');
const api_keys       = require('../config/api_keys');
const email_utils = require('./email_utils.js');

const fileTreatment = function(collection,dateFormatObjects) {
	let res = {}
	res.bool = false
	try {
		collection.forEach( function(obj,i,theArray){
			dateFormatObjects.forEach((dateFormat) => {
				theArray[i][dateFormat.columnName] =  (obj[dateFormat.columnName]) ? moment(obj[dateFormat.columnName]).format(dateFormat.format) : ''
			})
		})
		collection.forEach( function(obj,i,theArray){        theArray[i] =  flatten(obj) })

		res.bool = true
		res.collection = collection
		console.log('Good: File formated !')
	} catch (err) {			console.log('Error trying to format') }
	return res
};


const deleteFields = function(collection,fieldsArray) {
	try {
		collection.forEach( function(obj,i,theArray){
			fieldsArray.forEach((field) => {
				if(obj.hasOwnProperty(field)){delete obj[field];}
			})
			return obj
		})
		console.log('Good: Fields properly deleted !')
	} catch (err) {			console.log('Error trying to delete fields'); }
	return collection
};

let fileParse = function(collection){
  let opts;
  let debut = new Date().getTime()

	try {
    const headers = buildCSVHeader(collection);
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
    const parser = new Json2csvParser(opts);
    const csv = parser.parse(collection);
    console.log('csv finished');
    console.log((new Date().getTime() - debut) / 1000  );
    return csv
  } catch (err) {    console.error('err in Json2csvParser',err);
  }
  return false
}



const flatten = (objectOrArray, prefix = '', formatter = (k) => (k)) => {
  const nestedFormatter = (k) => ('/' + k)
  const nestElement = (prev, value, key) => (
    (value && typeof value === 'object')
      ? { ...prev, ...flatten(value, `${prefix}${formatter(key)}`, nestedFormatter) }
      : { ...prev, ...{ [`${prefix}${formatter(key)}`]: value } });

  return Array.isArray(objectOrArray)
    ? objectOrArray.reduce(nestElement, {})
    : Object.keys(objectOrArray).reduce(
      (prev, element) => nestElement(prev, objectOrArray[element], element),
      {},
    );
};


const buildCSVHeader = function(array) {
  let set = new Set();
  array.forEach(function(obj){
    Object.keys(obj).forEach(function(key){
      set.add(key)
    })
  })
  //sorting the columns
  let arr = [...set].sort();

  return arr
}



const wetransferProcess = async function(finalResults,name,email,res){
    const wtClient = await createWTClient(api_keys.wetransfer);
    const content = await Buffer.from(finalResults);

    if(content.length <= 1) {res.send('ERROR: content.length <= 1')}
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

    res.send(responseObj)
}

const writeAFile = function(file,fileInputFullPath){
	return fs.writeFileSync(fileInputFullPath, JSON.stringify(file));
}

const deleteOldFiles = function(){
	console.log('deleteOldFiles');
	let old = new Date();
	old.setHours(old.getHours()-1);
	let oldNumber = old.getTime()

	fs.readdir('./tmp/', function (err, files) {
	    if (err) {
	        return console.log('Unable to scan directory: ' + err);
	    }
	    files.forEach(function (file) {
				console.log(file);
				let date = parseInt(file.replace(/\D/,''))
				console.log(date);
				// si le fichier a ete creee il ya plus d'1 heure, alors on supprime
				if( date < oldNumber) {
					console.log('./tmp/'+file);
					fs.unlink('./tmp/'+file, (err) => {
					  if (err) throw err;
					  console.log(file,' was deleted');
					});
				}
	    });
	});

}

module.exports.fileParse = fileParse
module.exports.fileTreatment = fileTreatment
module.exports.deleteFields = deleteFields
module.exports.wetransferProcess = wetransferProcess
module.exports.writeAFile = writeAFile
module.exports.deleteOldFiles = deleteOldFiles
module.exports.buildCSVHeader =  buildCSVHeader
module.exports.flatten =  flatten
