const Json2csvParser = require('json2csv').Parser;
const moment = require('moment');

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


let fileParse = function(collection){
  let opts;
  let debut = new Date().getTime()

	try {
    const headers = buildCSVHeader(collection);
    opts = {
      fields:headers,
      delimiter: ';',
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


module.exports.fileParse = fileParse
module.exports.fileTreatment = fileTreatment
