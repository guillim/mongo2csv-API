# MicroService for fetching data from a remote MongoDB database, and format it into CSV.

### Prerequisite:
You need to install Node 8.0 or higher. [see more](https://nodejs.org/en/download/)

## Setup:
In your terminal, type:  
```
git clone https://github.com/guillim/mongo2csv-API.git mongo2csv-API && cd mongo2csv-API && npm install
```  

Then configure your database:  
In the /config folder, rename _db_example.js_ into _db.js_ and fill with your credentials / IPs... Some examples are commented inside this file.
## Run:
```bash
npm run dev
```
Here you go ! Follow this link to see it working: http://localhost:8000/status

## MongoDB Configuration if you haven't any:
1. Go to https://mlab.com/ and create a free account (you have 500 Mo for free, enough for testing ! )
2. Create a database, and remember the name you chose
3. Add a user and a password, and copy the "mongod" URL
4. In the /config folder, rename _db_example.js_ into _db.js_ and replace password, username and databasename by the one you just created

![instructions](https://ibin.co/4GjY8K0VS5kf.png "Instructions to set up the free database")

## Customise your query to MongoDB
I wrote an example of  MongoDB query in the file note_routes.js
To fit you database Model, simply edit this file, and here you go !

----------------------------



### Thanks [Scott](https://github.com/scottdomes)
This was built following the [excellent article](https://medium.freecodecamp.org/building-a-simple-node-js-api-in-under-30-minutes-a07ea9e390d2)
