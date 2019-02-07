const express        = require('express');
const MongoClient    = require('mongodb').MongoClient;
const bodyParser     = require('body-parser');
const db             = require('./config/db');


// initialise the app
const app            = express();

// Set up the port we want to listen to for this app
const port = 8000;
app.listen(port, () => {
  console.log('We are live on port:' + port);
});

// Unfortunately, Express can’t process "x-www-form-urlencoded" post form on its own. body-parser package helps fot that [needs to be before  routes definition]
app.use(bodyParser.urlencoded({ extended: true }));



// Set up the database, and everything from here
MongoClient.connect(db.url, (err, database) => {

  // If problem, then we want to set up a unique route, that we will use to check the API status
  if (err) {
    app.get('/status', (req, res) => {
      res.send('Not connected to the database')
    });
    console.log(err)
  }else{
    // setting up all the routes defined in the *routes* folder - make sure you add the database name (here = "test_database") and not a collection name
    require('./app/routes')(app, database.db(db.databasename));
  }
})
