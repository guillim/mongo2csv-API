// here you define the connector to your mongodb database
let databasename = 'databasename'
module.exports = {
  // If in localhost:
  // url : 'mongodb://127.0.0.1:3001',
  // If with Mlab - you can set up a MongoDB database hosted on Mlab, add a user and a password, and paste the corresponding URL here to connect the MongoDB databse:

  //For instance if your database is on a server at this adress 34.131.87.116, and your database is open on port 27017. Let's say your IP (well the IP of the server on which you install this app) is 24.141.27.176
  //First don't forget to allow incoming connection from this server to you database. Simply connect to you databse server using SSH and type: sudo ufw allow from 24.141.27.176 to any port 27017
  //then this should work
  url : 'mongodb://34.131.87.116:27017/'+databasename,
  databasename: databasename
};
